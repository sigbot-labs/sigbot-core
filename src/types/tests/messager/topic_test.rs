use sigbot_types::messager::topic::AccountTopic;
use sigbot_types::messager::topic::MessageScope;
use sigbot_types::messager::topic::TopicAddress;
use sigbot_types::messager::topic::WorkflowTopic;
use sigbot_types::Uuid;

#[test]
fn all_topics_expand_and_roundtrip_without_aliases() {
    let scope = MessageScope {
        env: "paper".into(),
        tenant_id: Uuid::parse_str("0192f7b2-5c00-7000-8000-000000000111").unwrap(),
    };
    let workflow_id = Uuid::parse_str("0192f7b2-5c00-7000-8000-000000000112").unwrap();
    let account_id = Uuid::parse_str("0192f7b2-5c00-7000-8000-000000000113").unwrap();
    let mut topics = std::collections::HashSet::new();
    for kind in WorkflowTopic::ALL {
        let topic = kind.topic(&scope, workflow_id).unwrap();
        assert!(topics.insert(topic.clone()));
        assert_eq!(
            TopicAddress::parse(&topic).unwrap(),
            TopicAddress::Workflow {
                scope: scope.clone(),
                workflow_id,
                kind
            }
        );
    }
    for kind in AccountTopic::ALL {
        let topic = kind.topic(&scope, account_id).unwrap();
        assert!(topics.insert(topic.clone()));
        assert_eq!(
            TopicAddress::parse(&topic).unwrap(),
            TopicAddress::Account {
                scope: scope.clone(),
                account_id,
                kind
            }
        );
    }
    assert_eq!(topics.len(), 22);
}

#[test]
fn identity_rejects_wildcards_path_injection_and_invalid_ids() {
    let tenant_id = Uuid::parse_str("0192f7b2-5c00-7000-8000-000000000111").unwrap();
    let workflow_id = Uuid::parse_str("0192f7b2-5c00-7000-8000-000000000112").unwrap();
    for env in ["", "+", "#", "paper/live", "paper\0"] {
        assert!(MessageScope {
            env: env.into(),
            tenant_id,
        }
        .prefix()
        .is_err());
    }
    for topic in [
        format!("/internal/v2/paper/{tenant_id}/workflow/{workflow_id}/feed/news"),
        format!("internal/v2/paper/+/workflow/{workflow_id}/feed/news"),
        format!("internal/v2/paper/{tenant_id}/workflow/00000000-0000-0000-0000-000000000000/feed/news"),
        format!("internal/v2/paper/{tenant_id}/workflow/{workflow_id}/feed/unknown"),
    ] {
        assert!(TopicAddress::parse(&topic).is_err());
    }
}
