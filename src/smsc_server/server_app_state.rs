use crate::smsc_server::client_manager::ClientManager;

pub struct ServerAppState {
    pub(crate) client_manager: ClientManager,
}

impl ServerAppState {
    pub fn new(client_manager: ClientManager) -> Self {
        ServerAppState { client_manager }
    }

    pub fn get_client_manager(&self) -> &ClientManager {
        &self.client_manager
    }
}
