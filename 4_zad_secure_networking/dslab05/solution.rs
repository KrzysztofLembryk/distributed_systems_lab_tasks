use rustls::{ClientConnection, RootCertStore, pki_types::{ServerName, pem::PemObject}};
use std::{io::{Read, Write}, sync::Arc};
// You can add here other imports from std or crates listed in Cargo.toml.
use hmac::{Hmac, Mac};
use sha2::Sha256;
use crate::certs;
// The below `PhantomData` marker is here only to suppress the "unused type
// parameter" error. Remove it when you implement your solution:
use std::marker::PhantomData;

type HmacSha256 = Hmac<Sha256>;
pub struct SecureClient<L: Read + Write> {
    // Add here any fields you need.
    mac: HmacSha256,
    tls_stream: rustls::StreamOwned<ClientConnection, L>
}

pub struct SecureServer<L: Read + Write> {
    // Add here any fields you need.
    phantom: PhantomData<L>,
}

struct MsgFormat
{
    len: u32,
    content: Vec<u8>,
    hmac: HmacSha256
}

impl MsgFormat
{
    pub fn new(data: &Vec<u8>)
    {

    }
}

impl<L: Read + Write> SecureClient<L> {
    /// Creates a new instance of `SecureClient`.
    ///
    /// `SecureClient` communicates with `SecureServer` via `link`.
    /// The messages include a HMAC tag calculated using `hmac_key`.
    /// A certificate of `SecureServer` is signed by `root_cert`.
    /// We are connecting with `server_hostname`.
    pub fn new(
        link: L,
        hmac_key: &[u8],
        root_cert: &str,
        server_hostname: ServerName<'static>,
    ) -> Self 
    {
        // Initialize a new MAC instance from the hmac key:
        let mac = HmacSha256::new_from_slice(hmac_key).unwrap();
        let mut root_store = RootCertStore::empty();

        // Add to the store the root certificate of the server:
        root_store.add_parsable_certificates(
            rustls::pki_types::CertificateDer::from_pem_slice(root_cert.as_bytes()),
        );

        // Create a TLS configuration for the client:
        let client_config = rustls::ClientConfig::builder()
                            .with_root_certificates(root_store)
                            .with_no_client_auth();

        // Create a TLS connection using the configuration prepared above.
        let tls_conn = ClientConnection::new(
                            Arc::new(client_config), 
                            server_hostname).unwrap();
        let tls_stream = rustls::StreamOwned::new(tls_conn, link);
        
        SecureClient { 
            mac: mac, 
            tls_stream,
        }
    }

    /// Sends the data to the server. The sent message follows the
    /// format specified in the description of the assignment.
    pub fn send_msg(&mut self, data: Vec<u8>) 
    {
        let data_len: u32 = data.len() as u32;
        // we need to make clone since finalize consumes mac 
        let mut mac = self.mac.clone();

        mac.update(&data);

        let tag = mac.finalize().into_bytes();
        let tag_buf: [u8; 32] = tag.into();
        let mut buf: Vec<u8> = Vec::new();

        buf.extend_from_slice(&data_len.to_be_bytes());
        buf.extend_from_slice(&data);
        buf.extend_from_slice(&tag_buf);

        self.tls_stream.write_all(&mut buf).unwrap();
    }
}

impl<L: Read + Write> SecureServer<L> {
    /// Creates a new instance of `SecureServer`.
    ///
    /// `SecureServer` receives messages from `SecureClients` via `link`.
    /// HMAC tags of the messages are verified against `hmac_key`.
    /// The private key of the `SecureServer`'s certificate is `server_private_key`,
    /// and the full certificate chain is `server_full_chain`.
    pub fn new(
        link: L,
        hmac_key: &[u8],
        server_private_key: &str,
        server_full_chain: &str,
    ) -> Self {
        unimplemented!()
    }

    /// Receives the next incoming message and returns the message's content
    /// (i.e., without the message size and without the HMAC tag) if the
    /// message's HMAC tag is correct. Otherwise, returns `SecureServerError`.
    pub fn recv_message(&mut self) -> Result<Vec<u8>, SecureServerError> {
        unimplemented!()
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum SecureServerError {
    /// The HMAC tag of a message is invalid.
    InvalidHmac,
}

// You can add any private types, structs, consts, functions, methods, etc., you need.
