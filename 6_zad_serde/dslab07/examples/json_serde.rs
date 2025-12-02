use std::collections::HashMap;
use std::io::stdout;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Person {
    name: String,
    age: u8,
    phones: Vec<String>,
}

// Based on serde_json examples.
fn parsed_example() {
    // Some JSON input data as a &str. Maybe this comes from the user.
    let data = r#"
        {
            "name": "John Doe",
            "age": 43,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        }"#;

    // Parse the string of data into a Person object.
    let p: Person = serde_json::from_str(data).unwrap();

    // Do things just like with any other Rust data structure.
    println!("Please call {} at the number {}", p.name, p.phones[0]);
}

fn encoding_example() {
    // Starting from a Rust struct...
    let p = Person {
        name: "John Doe".to_string(),
        age: 42,
        phones: vec![],
    };
    // We can encode Serialize-able data into a `String`
    let json = serde_json::to_string(&p).unwrap();
    println!("Encoded JSON: {json}");
}

fn parsing_error() {
    // What if a value is out of range?
    let data = r#"
        {
            "name": "John Doe",
            "age": 256,
            "phones": []
        }"#;

    println!(
        "Parsing error: {:?}",
        serde_json::from_str::<'_, Person>(data).err()
    );
}

fn serializing_complex_type() {
    // Complex types implement `Serialize` based on field implementations,
    // just like `std::fmt::Debug`.
    #[derive(Debug, Serialize, Deserialize)]
    // We can instruct the representation with serde directives
    #[serde(tag = "type", content = "inside")]
    enum Variant {
        Map(HashMap<String, Variant>),
        #[serde(rename = "memory")]
        Array([i32; 4]),
        Structure {
            #[serde(skip_serializing)] // No need to serialize
            noop_field: (),
        },
        Unit,
    }

    let data = Variant::Map(
        [
            ("U".to_string(), Variant::Unit),
            ("Arr".to_string(), Variant::Array([1, 2, 3, 4])),
            ("S".to_string(), Variant::Structure { noop_field: () }),
            ("Nest".to_string(), Variant::Map(HashMap::new())),
        ]
        .into(),
    );
    // We can even print the data directly to stdout
    serde_json::to_writer_pretty(stdout(), &data).unwrap();
}

fn main() {
    parsed_example();
    encoding_example();
    parsing_error();
    serializing_complex_type();
}
