use std::{
    fmt::{Debug, Write as _},
    panic::{AssertUnwindSafe, catch_unwind},
};

use ctxmux_protocol::{
    ClientFrame, ClientHello, DaemonInstanceId, PROTOCOL_VERSION, RuntimeBuildId, RuntimeId,
    RuntimeIdPersistence, RuntimeIdentity, ServerFrame, decode_frame, encode_frame,
};
use serde::{Serialize, de::DeserializeOwned};

#[test]
fn seeded_native_protocol_fuzz_target_is_total_and_round_trips_valid_frames() {
    let initial_seed = environment_u64("CTXMUX_FUZZ_SEED", 0x4e44_4a53_4f4e);
    let mut state = initial_seed;
    let cases = usize::try_from(environment_u64("CTXMUX_FUZZ_CASES", 512))
        .expect("fuzz case count fits usize");
    assert!(cases > 0, "fuzz case count must be positive");
    let valid_client = encode_frame(&ClientFrame::Hello {
        hello: ClientHello {
            protocol: PROTOCOL_VERSION,
        },
    })
    .expect("encode valid client seed")
    .into_bytes();
    let valid_server = encode_frame(&ServerFrame::Hello {
        runtime: RuntimeIdentity {
            daemon_instance_id: DaemonInstanceId::new(),
            runtime_id: RuntimeId::new(),
            runtime_id_persistence: RuntimeIdPersistence::Daemon,
            build_id: RuntimeBuildId::new("ctxmuxd/fuzz").unwrap(),
            protocol_generation: PROTOCOL_VERSION,
            platform: "linux".to_owned(),
            arch: "x86_64".to_owned(),
            capabilities: std::collections::BTreeMap::from([("native.start".to_owned(), 1)]),
        },
    })
    .expect("encode valid server seed")
    .into_bytes();
    assert_round_trip::<ClientFrame>(&valid_client);
    assert_round_trip::<ServerFrame>(&valid_server);
    assert!(decode_frame::<ClientFrame>(b"{").is_err());
    assert!(decode_frame::<ServerFrame>(b"{").is_err());

    let mut templates = malformed_protocol_frames();
    templates.push(valid_client);
    templates.push(valid_server);

    for case_index in 0..cases {
        let bytes = fuzz_case(&mut state, case_index, &templates);
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            assert_round_trip_if_decodes::<ClientFrame>(&bytes);
            assert_round_trip_if_decodes::<ServerFrame>(&bytes);
            assert_round_trip_if_decodes::<serde_json::Value>(&bytes);
        }));
        assert!(
            outcome.is_ok(),
            "native protocol fuzz panic: seed={initial_seed} case={case_index} prefix={}",
            byte_prefix(&bytes)
        );
    }
    println!("native protocol fuzz replay: seed={initial_seed} cases={cases}");
}

fn malformed_protocol_frames() -> Vec<Vec<u8>> {
    let corpus: serde_json::Value = serde_json::from_str(include_str!(
        "../../../fixtures/malformed-protocol-frames.json"
    ))
    .expect("parse shared malformed-frame corpus");
    corpus["frames"]
        .as_array()
        .expect("corpus frames are an array")
        .iter()
        .map(|frame| {
            frame["bytes"]
                .as_array()
                .expect("frame bytes are an array")
                .iter()
                .map(|byte| {
                    u8::try_from(byte.as_u64().expect("frame byte is an unsigned integer"))
                        .expect("frame byte fits in u8")
                })
                .collect()
        })
        .collect()
}

fn assert_round_trip<T>(bytes: &[u8])
where
    T: Debug + DeserializeOwned + PartialEq + Serialize,
{
    let decoded = decode_frame::<T>(bytes).expect("valid seed decodes");
    let encoded = encode_frame(&decoded).expect("valid seed re-encodes");
    assert_eq!(
        decode_frame::<T>(&encoded).expect("re-encoded valid seed decodes"),
        decoded
    );
}

fn assert_round_trip_if_decodes<T>(bytes: &[u8])
where
    T: Debug + DeserializeOwned + PartialEq + Serialize,
{
    let Ok(decoded) = decode_frame::<T>(bytes) else {
        return;
    };
    let encoded = encode_frame(&decoded).expect("decoded value re-encodes within the frame cap");
    assert_eq!(
        decode_frame::<T>(&encoded).expect("re-encoded value decodes"),
        decoded
    );
}

fn fuzz_case(state: &mut u64, case_index: usize, templates: &[Vec<u8>]) -> Vec<u8> {
    if case_index.is_multiple_of(3) {
        let template_index =
            usize::try_from(next_random(state)).expect("random value fits usize") % templates.len();
        let mut bytes = templates[template_index].clone();
        let mutation_count = 1 + next_random(state) % 8;
        for _ in 0..mutation_count {
            match next_random(state) % 3 {
                0 if !bytes.is_empty() => {
                    let index = usize::try_from(next_random(state))
                        .expect("random value fits usize")
                        % bytes.len();
                    bytes[index] ^= (next_random(state) & 0xff) as u8;
                }
                1 => {
                    let index = usize::try_from(next_random(state))
                        .expect("random value fits usize")
                        % (bytes.len() + 1);
                    bytes.insert(index, (next_random(state) & 0xff) as u8);
                }
                _ if !bytes.is_empty() => {
                    let index = usize::try_from(next_random(state))
                        .expect("random value fits usize")
                        % bytes.len();
                    bytes.remove(index);
                }
                _ => {}
            }
        }
        return bytes;
    }

    let length = usize::try_from(next_random(state) % 2049).expect("bounded length fits usize");
    (0..length)
        .map(|_| (next_random(state) & 0xff) as u8)
        .collect()
}

fn byte_prefix(bytes: &[u8]) -> String {
    let mut prefix = String::with_capacity(bytes.len().min(64) * 2);
    for byte in bytes.iter().take(64) {
        write!(&mut prefix, "{byte:02x}").expect("writing to a String cannot fail");
    }
    prefix
}

fn environment_u64(name: &str, default: u64) -> u64 {
    std::env::var(name).map_or(default, |value| {
        value
            .parse::<u64>()
            .unwrap_or_else(|error| panic!("{name} must be an unsigned integer: {error}"))
    })
}

fn next_random(state: &mut u64) -> u64 {
    if *state == 0 {
        *state = 0x9e37_79b9_7f4a_7c15;
    }
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}
