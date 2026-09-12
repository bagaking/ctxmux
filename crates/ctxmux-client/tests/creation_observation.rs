use ctxmux_client::{Client, ClientError};
use ctxmux_protocol::{
    ClientFrame, CreateOperationKey, CreationObservation, DaemonInstanceId, PROTOCOL_VERSION,
    RUNTIME_CAPABILITY_CREATION_OBSERVATION, Response, RuntimeBuildId, RuntimeId,
    RuntimeIdPersistence, RuntimeIdentity, ServerFrame, decode_frame, encode_frame,
};
use futures_util::{SinkExt, StreamExt};
use std::collections::BTreeMap;
use tokio::net::UnixListener;
use tokio_util::codec::{Framed, LinesCodec};

fn identity(capable: bool) -> RuntimeIdentity {
    RuntimeIdentity {
        daemon_instance_id: DaemonInstanceId::new(),
        runtime_id: RuntimeId::new(),
        runtime_id_persistence: RuntimeIdPersistence::StateDir,
        build_id: RuntimeBuildId::new("ctxmuxd/test").unwrap(),
        protocol_generation: PROTOCOL_VERSION,
        platform: "macos".into(),
        arch: "aarch64".into(),
        capabilities: if capable {
            BTreeMap::from([(RUNTIME_CAPABILITY_CREATION_OBSERVATION.to_owned(), 1)])
        } else {
            BTreeMap::new()
        },
    }
}

#[tokio::test]
async fn creation_observation_missing_capability_is_local_and_sends_no_request() {
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("r.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    let runtime = identity(false);
    let peer = tokio::spawn(async move {
        for attempt in 0..2 {
            let (stream, _) = listener.accept().await.unwrap();
            let mut wire = Framed::new(stream, LinesCodec::new());
            let _: ClientFrame = decode_frame(wire.next().await.unwrap().unwrap()).unwrap();
            wire.send(
                encode_frame(&ServerFrame::Hello {
                    runtime: runtime.clone(),
                })
                .unwrap(),
            )
            .await
            .unwrap();
            if attempt == 0 {
                assert!(wire.next().await.is_none());
            } else {
                let frame: ClientFrame = decode_frame(wire.next().await.unwrap().unwrap()).unwrap();
                assert!(matches!(
                    frame,
                    ClientFrame::Request {
                        request: ctxmux_protocol::Request::List { .. }
                    }
                ));
                wire.send(
                    encode_frame(&ServerFrame::Response {
                        response: Response::Runs {
                            runs: Vec::new(),
                            next_cursor: None,
                        },
                    })
                    .unwrap(),
                )
                .await
                .unwrap();
            }
        }
    });
    let client = Client::new(&socket);
    let key = CreateOperationKey::new("original").unwrap();
    assert!(
        matches!(client.observe_creation(key).await.unwrap_err(), ClientError::UnsupportedCapability { capability, .. } if capability == RUNTIME_CAPABILITY_CREATION_OBSERVATION)
    );
    assert!(client.list().await.unwrap().is_empty());
    peer.await.unwrap();
}

#[tokio::test]
async fn creation_observation_uses_same_hello_and_rejects_another_echoed_key() {
    for wrong_key in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("r.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let runtime = identity(true);
        let expected = runtime.clone();
        let key = CreateOperationKey::new("original").unwrap();
        let requested = key.clone();
        let peer = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut wire = Framed::new(stream, LinesCodec::new());
            let _: ClientFrame = decode_frame(wire.next().await.unwrap().unwrap()).unwrap();
            wire.send(encode_frame(&ServerFrame::Hello { runtime }).unwrap())
                .await
                .unwrap();
            let frame: ClientFrame = decode_frame(wire.next().await.unwrap().unwrap()).unwrap();
            assert!(
                matches!(frame, ClientFrame::Request { request: ctxmux_protocol::Request::ObserveCreation { operation_key } } if operation_key == requested)
            );
            let observation = CreationObservation::NotRetained {
                operation_key: if wrong_key {
                    CreateOperationKey::new("neighbor").unwrap()
                } else {
                    requested
                },
            };
            wire.send(
                encode_frame(&ServerFrame::Response {
                    response: Response::CreationObservation { observation },
                })
                .unwrap(),
            )
            .await
            .unwrap();
        });
        let result = Client::new(&socket).observe_creation(key.clone()).await;
        if wrong_key {
            assert!(matches!(
                result.unwrap_err(),
                ClientError::UnexpectedFrame(_)
            ));
        } else {
            let (actual, observation) = result.unwrap();
            assert_eq!(actual, expected);
            assert_eq!(observation.operation_key(), &key);
        }
        peer.await.unwrap();
    }
}
