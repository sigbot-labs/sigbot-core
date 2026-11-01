// SPDX-License-Identifier: GPL-3.0-only

use crate::modules::messager::storage::postgres::MessagerPostgresRepository;
use anyhow::ensure;
use anyhow::Result;
use chrono::DateTime;
use chrono::Utc;
use serde_json::json;
use serde_json::Value;
use sigbot_types::messager::envelope::DurableMessage;
use sigbot_types::messager::envelope::MessageEnvelope;
use sigbot_types::strategy::execution::CaseReference;
use sigbot_types::strategy::execution::HyperparametersUpdate;
use sqlx::Postgres;
use sqlx::Row;
use sqlx::Transaction;
use uuid::Uuid;

pub struct EvaluationEvidence {
    pub facts: Vec<Value>,
    pub cases: Vec<Value>,
    pub case_refs: Vec<CaseReference>,
}

pub struct EvaluationClaim<'a> {
    consumer: String,
    source: DurableMessage<Value>,
    transaction: Transaction<'a, Postgres>,
}

impl EvaluationClaim<'_> {
    pub async fn evidence(&mut self, as_of: DateTime<Utc>) -> Result<EvaluationEvidence> {
        let scope = &self.source.scope;
        let rows = sqlx::query(
            "SELECT source,case_id,available_at,facts,inference,provenance FROM eval_case
            WHERE env=$1 AND tenant_id=$2 AND available_at<=$3 AND observed_at<=$3
            ORDER BY available_at DESC,source,case_id LIMIT 32",
        )
        .bind(&scope.env)
        .bind(scope.tenant_id)
        .bind(as_of)
        .fetch_all(&mut *self.transaction)
        .await?;
        let mut case_refs = Vec::new();
        let mut cases = Vec::new();
        for row in rows {
            let reference = CaseReference {
                source: row.get("source"),
                case_id: row.get("case_id"),
                available_time: row.get("available_at"),
            };
            cases.push(json!({"reference": reference, "facts": row.get::<Value,_>("facts"),
                "inferences": row.get::<Value,_>("inference"), "provenance": row.get::<Value,_>("provenance")}));
            case_refs.push(reference);
        }
        let rows = sqlx::query(
            "SELECT source,source_event_id,symbol,event_at,available_at,payload FROM mkt_event
            WHERE env=$1 AND tenant_id=$2 AND run_id=$3 AND available_at<=$4 AND event_at<=$4
            ORDER BY available_at DESC,source,source_event_id LIMIT 64",
        )
        .bind(&scope.env)
        .bind(scope.tenant_id)
        .bind(&self.source.run_id)
        .bind(as_of)
        .fetch_all(&mut *self.transaction)
        .await?;
        let facts = rows.into_iter().map(|row| json!({"source": row.get::<String,_>("source"),
            "source_event_id": row.get::<String,_>("source_event_id"), "symbol": row.get::<String,_>("symbol"),
            "event_time": row.get::<DateTime<Utc>,_>("event_at"), "available_time": row.get::<DateTime<Utc>,_>("available_at"),
            "payload": row.get::<Value,_>("payload")})).collect();
        Ok(EvaluationEvidence {
            facts,
            cases,
            case_refs,
        })
    }

    pub async fn next_version(&mut self, workflow_id: Uuid, node_id: i64) -> Result<i64> {
        let scope = &self.source.scope;
        Ok(sqlx::query_scalar(
            "SELECT COALESCE(MAX(version),0)+1 FROM eval_snapshot WHERE env=$1 AND tenant_id=$2
            AND workflow_id=$3 AND node_id=$4 AND run_id=$5",
        )
        .bind(&scope.env)
        .bind(scope.tenant_id)
        .bind(workflow_id)
        .bind(node_id)
        .bind(&self.source.run_id)
        .fetch_one(&mut *self.transaction)
        .await?)
    }

    pub async fn complete(mut self) -> Result<()> {
        MessagerPostgresRepository::complete(&mut self.transaction, &self.consumer, &self.source).await?;
        self.transaction.commit().await?;
        Ok(())
    }

    pub async fn publish(
        mut self,
        workflow_id: Uuid,
        node_id: i64,
        update: &HyperparametersUpdate,
        source_facts: &[Value],
        citations: &[Value],
        inferences: &[String],
        completed_at: DateTime<Utc>,
        topic: &str,
        output: &DurableMessage<Value>,
    ) -> Result<()> {
        let scope = &self.source.scope;
        sqlx::query(
            "INSERT INTO eval_snapshot(env,tenant_id,workflow_id,node_id,run_id,version,as_of,valid_until,
            probabilities,parameters,model_version,case_refs,facts,inferences,classification_version,available_at)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)",
        )
        .bind(&scope.env)
        .bind(scope.tenant_id)
        .bind(workflow_id)
        .bind(node_id)
        .bind(&self.source.run_id)
        .bind(i64::try_from(update.version)?)
        .bind(update.as_of)
        .bind(update.valid_until)
        .bind(serde_json::to_value(&update.probabilities)?)
        .bind(serde_json::to_value(&update.parameters)?)
        .bind(&update.model_version)
        .bind(serde_json::to_value(&update.case_refs)?)
        .bind(json!({"source_facts": source_facts, "model_citations": citations}))
        .bind(serde_json::to_value(inferences)?)
        .bind(&update.classification_version)
        .bind(completed_at)
        .execute(&mut *self.transaction)
        .await?;
        MessagerPostgresRepository::enqueue(&mut self.transaction, &self.consumer, topic, output).await?;
        MessagerPostgresRepository::complete(&mut self.transaction, &self.consumer, &self.source).await?;
        self.transaction.commit().await?;
        Ok(())
    }
}

pub struct EvaluatorPostgresRepository {
    pool: sqlx::PgPool,
}

impl EvaluatorPostgresRepository {
    pub async fn connect(url: &str) -> Result<Self> {
        Ok(Self {
            pool: sqlx::postgres::PgPoolOptions::new()
                .max_connections(4)
                .connect(url)
                .await?,
        })
    }

    pub fn message_store(&self) -> MessagerPostgresRepository {
        MessagerPostgresRepository::new(self.pool.clone())
    }

    pub async fn claim(
        &self,
        consumer: &str,
        topic: &str,
        message: &MessageEnvelope<Value>,
    ) -> Result<Option<EvaluationClaim<'_>>> {
        let mut transaction = self.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
            .bind(consumer)
            .execute(&mut *transaction)
            .await?;
        if !MessagerPostgresRepository::accept(&mut transaction, consumer, topic, message).await? {
            transaction.commit().await?;
            return Ok(None);
        }
        ensure!(!consumer.is_empty(), "evaluation consumer required");
        Ok(Some(EvaluationClaim {
            consumer: consumer.to_owned(),
            source: message.durable.clone(),
            transaction,
        }))
    }
}
