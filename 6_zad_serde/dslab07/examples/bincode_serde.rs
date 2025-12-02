use serde::{Deserialize, Serialize};

// Serde provides the `#[derive(Serialize, Deserialize)]` macros to
// automatically generate implementations of the `Serialize` and `Deserialize`
// traits for custom data structures. To use it, you need to add
// `features = ["derive"]`  to the Serde dependency in Cargo.toml:
#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct Entity {
    y: f32,
    x: i32,
}

fn main() {
    let entity = Entity { x: 1, y: -11.0 };

    // Serialize the object:
    let serialized: Vec<u8> =
        bincode::serde::encode_to_vec(&entity, bincode::config::standard()).unwrap();
    println!("Serialized entity: {serialized:?}");

    // Deserialize the object:
    let (deserialized, bytes_read): (Entity, usize) =
        bincode::serde::decode_from_slice(&serialized, bincode::config::standard()).unwrap();
    println!("The deserialization read {bytes_read} bytes.");
    assert_eq!(entity, deserialized);

    // Bincode supports customization of the binary format. For instance, we can use
    // a different integer encoding. Also, we use another encoding function:
    let mut buf = [0u8; 16];
    let bytes_written =
        bincode::serde::encode_into_slice(entity, &mut buf, bincode::config::legacy()).unwrap();
    println!("This encoding took {bytes_written} bytes.");
}
