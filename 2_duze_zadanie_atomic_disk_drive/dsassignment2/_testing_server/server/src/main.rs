use atomic_drive::{run_register_process, Configuration, PublicConfiguration};
use std::time::Duration;
use tempfile::tempdir;

#[tokio::main]
async fn main() 
{
    let hmac_client_key = [5; 32];
    let tcp_ports = [31210, 31211];
    let storage_dir2 = tempdir().unwrap();
    let storage_dir2_path = storage_dir2.path().to_path_buf();

    let config2 = Configuration {
        public: PublicConfiguration {
            tcp_locations: tcp_ports
                .iter()
                .map(|port| ("127.0.0.1".to_string(), *port))
                .collect(),
            self_rank: 2,
            n_sectors: 20,
            storage_dir: storage_dir2_path.clone(),
        },
        hmac_system_key: [1; 64],
        hmac_client_key,
    };

    run_register_process(config2).await;
}
