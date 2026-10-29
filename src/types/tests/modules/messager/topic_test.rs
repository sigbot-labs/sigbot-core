use sigbot_types::modules::messager::topic::{AccountTopic, MessageScope, TopicAddress, WorkflowTopic};

#[test]
fn all_topics_expand_and_roundtrip_without_aliases() {
    let scope = MessageScope {
        env: "paper".into(),
        tenant_id: 7,
        space_id: 11,
    };
    let mut topics = std::collections::HashSet::new();
    for kind in WorkflowTopic::ALL {
        let topic = kind.topic(&scope, 13).unwrap();
        assert!(topics.insert(topic.clone()));
        assert_eq!(
            TopicAddress::parse(&topic).unwrap(),
            TopicAddress::Workflow {
                scope: scope.clone(),
                workflow_id: 13,
                kind
            }
        );
    }
    for kind in AccountTopic::ALL {
        let topic = kind.topic(&scope, 17).unwrap();
        assert!(topics.insert(topic.clone()));
        assert_eq!(
            TopicAddress::parse(&topic).unwrap(),
            TopicAddress::Account {
                scope: scope.clone(),
                account_id: 17,
                kind
            }
        );
    }
    assert_eq!(topics.len(), 22);
}

#[test]
fn identity_rejects_wildcards_path_injection_and_nonpositive_ids() {
    for env in ["", "+", "#", "paper/live", "paper\0"] {
        assert!(MessageScope {
            env: env.into(),
            tenant_id: 1,
            space_id: 2
        }
        .prefix()
        .is_err());
    }
    for topic in [
        "/internal/v2/paper/1/2/workflow/3/feed/news",
        "internal/v2/paper/+/2/workflow/3/feed/news",
        "internal/v2/paper/1/2/workflow/0/feed/news",
        "internal/v2/paper/1/2/workflow/3/feed/unknown",
    ] {
        assert!(TopicAddress::parse(topic).is_err());
    }
}
