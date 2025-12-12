use rustls::{ClientConnection, RootCertStore, ServerConnection, StreamOwned, pki_types::{CertificateDer, ServerName, pem::PemObject}};
use std::{io::{Read, Write}, sync::Arc};
// You can add here other imports from std or crates listed in Cargo.toml.
use hmac::{Hmac, Mac};
use sha2::Sha256;
// The below `PhantomData` marker is here only to suppress the "unused type
// parameter" error. Remove it when you implement your solution:

type HmacSha256 = Hmac<Sha256>;
pub struct SecureClient<L: Read + Write> {
    // Add here any fields you need.
    mac: HmacSha256,
    tls_stream: rustls::StreamOwned<ClientConnection, L>
}

pub struct SecureServer<L: Read + Write> {
    // Add here any fields you need.
    mac: HmacSha256,
    tls_stream: rustls::StreamOwned<ServerConnection, L>
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
    ) -> Self 
    {
        let mac = HmacSha256::new_from_slice(hmac_key).unwrap();

        // Load the certificate chain for the server:
        let certs_chain: Vec<CertificateDer> =    rustls::pki_types::CertificateDer::pem_slice_iter(
                server_full_chain.as_bytes())
                .flatten()
                .collect();
            
        let private_key = rustls::pki_types::PrivateKeyDer
            ::from_pem_slice(server_private_key.as_bytes())
            .unwrap();
        
        let server_config = rustls::ServerConfig::builder()
                            .with_no_client_auth()
                            .with_single_cert(certs_chain, private_key)
                            .unwrap();

        let connection = rustls::ServerConnection::new(Arc::new(server_config)).unwrap();

        SecureServer { 
            mac: mac,            
            tls_stream: StreamOwned::new(connection, link)
        }
    }

    /// Receives the next incoming message and returns the message's content
    /// (i.e., without the message size and without the HMAC tag) if the
    /// message's HMAC tag is correct. Otherwise, returns `SecureServerError`.
    pub fn recv_message(&mut self) -> Result<Vec<u8>, SecureServerError> 
    {
        // Firstly we want to read size of data so that we can create big enough
        // buffer for this data
        let mut data_size = vec![0; 4];

        // read_exact will return error if it reads less than buff size
        self.tls_stream.read_exact(&mut data_size).unwrap();

        let data_size = u32::from_be_bytes(data_size[..].try_into().unwrap());

        // We read main data
        let mut buf = vec![0; data_size as usize];

        self.tls_stream.read_exact(&mut buf).unwrap();

        // We know mac is 32 bytes
        let mut tag_buf = vec![0; 32];

        self.tls_stream.read_exact(&mut tag_buf).unwrap();

        self.verify_hmac(&buf, &tag_buf.try_into().unwrap())?;

        Ok(buf)
    }

    fn verify_hmac(
        &self, 
        data: &Vec<u8>, 
        tag_buf: &[u8; 32]
    ) -> Result<(), SecureServerError>
    {
        let mut mac = self.mac.clone();

        // Calculate MAC for the data:
        mac.update(data);

        // Verify the tag:
        if mac.verify_slice(tag_buf).is_ok()
        {
            Ok(())
        }
        else 
        {
            Err(SecureServerError::InvalidHmac)
        }
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum SecureServerError {
    /// The HMAC tag of a message is invalid.
    InvalidHmac,
}

// You can add any private types, structs, consts, functions, methods, etc., you need.
