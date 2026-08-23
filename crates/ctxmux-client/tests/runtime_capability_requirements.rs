use std::{collections::BTreeMap, io, path::PathBuf, time::Duration};

use ctxmux_client::{Client, ClientError, RuntimeCapabilityRequirements};
use ctxmux_protocol::{
    ClientFrame, ClientHello, CommandDisposition, DaemonInstanceId, MAX_RUNTIME_CAPABILITY_VERSION,
    PROTOCOL_VERSION, RuntimeBuildId, RuntimeId, RuntimeIdPersistence, RuntimeIdentity,
    ServerFrame, decode_frame, encode_frame,
};
use futures_util::{SinkExt, StreamExt};
use tempfile::TempDir;
use tokio::{net::UnixListener, task::JoinHandle, time::timeout};
use tokio_util::codec::{Framed, LinesCodec, LinesCodecError};

#[test]
fn builder_rejects_versions_outside_the_shared_safe_integer_domain() {
    for required_version in [0, MAX_RUNTIME_CAPABILITY_VERSION + 1] {
        let error = Client::new("unused.sock")
            .with_required_capabilities(BTreeMap::from([(
                "candidate".to_owned(),
                required_version,
            )]))
            .expect_err("invalid requirement must fail at construction");
        assert!(matches!(
            error,
            ClientError::InvalidCapabilityRequirement {
                capability,
                required_version: actual,
                ..
            } if capability == "candidate" && actual == required_version
        ));
    }

    Client::new("unused.sock")
        .with_required_capabilities(BTreeMap::from([(
            "future.exact_key".to_owned(),
            MAX_RUNTIME_CAPABILITY_VERSION,
        )]))
        .expect("the maximum safe version and an unknown exact key are valid requirements");
}

#[tokio::test]
async fn request_requirement_rejects_an_absent_exact_key_before_dispatch() {
    let (directory, socket, peer) = mock_runtime(runtime_identity());
    let client = required_client(&socket, "toString", 1);

    let error = client
        .list()
        .await
        .expect_err("an inherited object name is still an absent exact key");
    assert_unsupported(
        error,
        "toString",
        1,
        None,
        Some(CommandDisposition::NotApplied),
    );
    assert_no_business_frame(directory, peer).await;
}

#[tokio::test]
async fn attach_requirement_rejects_a_higher_version_before_dispatch() {
    let (directory, socket, peer) = mock_runtime(runtime_identity());
    let client = required_client(&socket, "native.start", 2);

    let Err(error) = client.attach(ctxmux_protocol::RunId::new(), 0).await else {
        panic!("a higher requirement must reject Attach locally");
    };
    assert_unsupported(
        error,
        "native.start",
        2,
        Some(1),
        Some(CommandDisposition::NotApplied),
    );
    assert_no_business_frame(directory, peer).await;
}

#[tokio::test]
async fn control_requirement_is_typed_not_applied_before_dispatch() {
    let (directory, socket, peer) = mock_runtime(runtime_identity());
    let client = required_client(&socket, "native.start", 2);

    let error = client
        .input(ctxmux_protocol::RunId::new(), vec![1])
        .await
        .expect_err("a higher requirement must reject control locally");
    assert_unsupported(
        error,
        "native.start",
        2,
        Some(1),
        Some(CommandDisposition::NotApplied),
    );
    assert_no_business_frame(directory, peer).await;
}

#[tokio::test]
async fn expected_runtime_identity_fences_every_business_dispatch_path() {
    let expected = runtime_identity();
    let mut replacement = expected.clone();
    replacement.daemon_instance_id = DaemonInstanceId::new();

    let (directory, socket, peer) = mock_runtime(replacement.clone());
    let error = Client::new(&socket)
        .with_expected_runtime_identity(expected.clone())
        .list()
        .await
        .expect_err("a replacement Runtime must reject Request dispatch");
    assert_identity_mismatch(
        error,
        &expected,
        &replacement,
        Some(CommandDisposition::NotApplied),
    );
    assert_no_business_frame(directory, peer).await;

    let (directory, socket, peer) = mock_runtime(replacement.clone());
    let Err(error) = Client::new(&socket)
        .with_expected_runtime_identity(expected.clone())
        .attach(ctxmux_protocol::RunId::new(), 0)
        .await
    else {
        panic!("a replacement Runtime must reject Attach dispatch");
    };
    assert_identity_mismatch(
        error,
        &expected,
        &replacement,
        Some(CommandDisposition::NotApplied),
    );
    assert_no_business_frame(directory, peer).await;

    let (directory, socket, peer) = mock_runtime(replacement.clone());
    let error = Client::new(&socket)
        .with_expected_runtime_identity(expected.clone())
        .input(ctxmux_protocol::RunId::new(), vec![1])
        .await
        .expect_err("a replacement Runtime must reject control dispatch");
    assert_identity_mismatch(
        error,
        &expected,
        &replacement,
        Some(CommandDisposition::NotApplied),
    );
    assert_no_business_frame(directory, peer).await;
}

#[tokio::test]
async fn configured_ping_and_runtime_info_remain_raw_identity_inspection() {
    let runtime = runtime_identity();

    let (ping_directory, ping_socket, ping_peer) = mock_runtime(runtime.clone());
    required_client(&ping_socket, "services.persistent_state", 1)
        .ping()
        .await
        .expect("ping remains raw readiness inspection");
    assert_no_business_frame(ping_directory, ping_peer).await;

    let (info_directory, info_socket, info_peer) = mock_runtime(runtime.clone());
    let observed = required_client(&info_socket, "services.persistent_state", 1)
        .runtime_info()
        .await
        .expect("runtime_info remains raw identity inspection");
    assert_eq!(observed, runtime);
    assert_no_business_frame(info_directory, info_peer).await;

    let expected = runtime_identity();
    let mut replacement = expected.clone();
    replacement.daemon_instance_id = DaemonInstanceId::new();
    let (identity_directory, identity_socket, identity_peer) = mock_runtime(replacement.clone());
    let observed = Client::new(identity_socket)
        .with_expected_runtime_identity(expected)
        .runtime_info()
        .await
        .expect("runtime_info remains raw when the identity expectation differs");
    assert_eq!(observed, replacement);
    assert_no_business_frame(identity_directory, identity_peer).await;
}

fn required_client(socket: &PathBuf, capability: &str, version: u64) -> Client {
    let requirements: RuntimeCapabilityRequirements =
        BTreeMap::from([(capability.to_owned(), version)]);
    Client::new(socket)
        .with_required_capabilities(requirements)
        .expect("construct required-capability client")
}

fn runtime_identity() -> RuntimeIdentity {
    RuntimeIdentity {
        daemon_instance_id: DaemonInstanceId::new(),
        runtime_id: RuntimeId::new(),
        runtime_id_persistence: RuntimeIdPersistence::Daemon,
        build_id: RuntimeBuildId::new("ctxmuxd/test").unwrap(),
        protocol_generation: PROTOCOL_VERSION,
        platform: "linux".to_owned(),
        arch: "x86_64".to_owned(),
        capabilities: BTreeMap::from([("native.start".to_owned(), 1)]),
    }
}

fn mock_runtime(runtime: RuntimeIdentity) -> (TempDir, PathBuf, JoinHandle<Option<ClientFrame>>) {
    let directory = tempfile::tempdir().expect("create mock Runtime directory");
    let socket = directory.path().join("ctxmux.sock");
    let listener = UnixListener::bind(&socket).expect("bind mock Runtime socket");
    let peer = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept client connection");
        let mut wire = Framed::new(stream, LinesCodec::new());
        let hello = wire
            .next()
            .await
            .expect("client sends Hello")
            .expect("read client Hello");
        assert_eq!(
            decode_frame::<ClientFrame>(&hello).expect("decode client Hello"),
            ClientFrame::Hello {
                hello: ClientHello {
                    protocol: PROTOCOL_VERSION,
                },
            }
        );
        wire.send(encode_frame(&ServerFrame::Hello { runtime }).expect("encode Runtime identity"))
            .await
            .expect("send Runtime identity");

        match timeout(Duration::from_secs(5), wire.next())
            .await
            .expect("client settles the admitted connection")
        {
            None => None,
            Some(Ok(frame)) => Some(decode_frame(&frame).expect("decode business frame")),
            Some(Err(LinesCodecError::Io(error)))
                if matches!(
                    error.kind(),
                    io::ErrorKind::ConnectionReset | io::ErrorKind::BrokenPipe
                ) =>
            {
                None
            }
            Some(Err(error)) => panic!("mock Runtime transport failed: {error}"),
        }
    });
    (directory, socket, peer)
}

async fn assert_no_business_frame(_directory: TempDir, peer: JoinHandle<Option<ClientFrame>>) {
    assert_eq!(peer.await.expect("mock Runtime task completes"), None);
}

fn assert_unsupported(
    error: ClientError,
    expected_capability: &str,
    expected_required: u64,
    expected_advertised: Option<u64>,
    expected_disposition: Option<CommandDisposition>,
) {
    assert_eq!(error.control_disposition(), expected_disposition);
    assert!(matches!(
        error,
        ClientError::UnsupportedCapability {
            capability,
            required_version,
            advertised_version,
        } if capability == expected_capability
            && required_version == expected_required
            && advertised_version == expected_advertised
    ));
}

fn assert_identity_mismatch(
    error: ClientError,
    expected: &RuntimeIdentity,
    actual: &RuntimeIdentity,
    expected_disposition: Option<CommandDisposition>,
) {
    assert_eq!(error.control_disposition(), expected_disposition);
    assert!(matches!(
        error,
        ClientError::RuntimeIdentityMismatch {
            expected: rejected_expected,
            actual: rejected_actual,
        } if rejected_expected.as_ref() == expected && rejected_actual.as_ref() == actual
    ));
}
