use std::sync::Arc;
use crate::client::metadata::Metadata;
use crate::metadata::{JsonSerde, TableDescriptor, TableInfo, TablePath, DatabaseDescriptor};
use crate::rpc::{RpcClient, ServerConnection};
use crate::rpc::message::{GetTableRequest, CreateTableRequest, DropTableRequest, CreateDatabaseRequest, ListTablesRequest};


use crate::error::Result;
use crate::proto::{ GetTableInfoResponse};

pub struct FlussAdmin {
    admin_gateway: ServerConnection,
    metadata: Arc<Metadata>,
    rpc_client: Arc<RpcClient>,
}

impl FlussAdmin {
    pub async fn new(connections: Arc<RpcClient>, metadata: Arc<Metadata>) -> Result<Self> {
        let admin_con = connections
            .get_connection(
                metadata
                    .get_cluster()
                    .get_coordinator_server()
                    .expect("Couldn't coordinator server"),
            )
            .await?;

        Ok(FlussAdmin {
            admin_gateway: admin_con,
            metadata,
            rpc_client: connections,
        })
    }

    pub async fn create_database(
        &self,
        database_name: &str,
        ignore_if_exists: bool,
        database_descriptor: Option<&DatabaseDescriptor>,
    ) -> Result<()> {
        let _response = self
            .admin_gateway
            .request(CreateDatabaseRequest::new(
                database_name,
                ignore_if_exists,
                database_descriptor,
            )?)
            .await?;
        Ok(())
    }

    pub async fn create_table(
        &self,
        table_path: &TablePath,
        table_descriptor: &TableDescriptor,
        ignore_if_exists: bool,
    ) -> Result<()> {
        let response = self
            .admin_gateway
            .request(CreateTableRequest::new(
                table_path,
                table_descriptor,
                ignore_if_exists,
            )?)
            .await?;
        Ok(())
    }

    pub async fn drop_table(
        &self,
        table_path: &TablePath,
        ignore_if_exists: bool,
    ) -> Result<()> {
        let response = self
            .admin_gateway
            .request(DropTableRequest::new(
                table_path,
                ignore_if_exists,
            )?)
            .await?;
        Ok(())
    }

    pub async fn get_table(&self, table_path: &TablePath) -> Result<TableInfo> {
        let response = self
            .admin_gateway
            .request(GetTableRequest::new(table_path))
            .await?;
        let GetTableInfoResponse {
            table_id,
            schema_id,
            table_json,
            created_time,
            modified_time,
        } = response;
        let v: &[u8] = &table_json[..];
        let table_descriptor =
            TableDescriptor::deserialize_json(&serde_json::from_slice(v).unwrap())?;
        Ok(TableInfo::of(
            table_path.clone(),
            table_id,
            schema_id,
            table_descriptor,
            created_time,
            modified_time,
        ))
    }

    pub async fn list_tables(&self, database_name: &str) -> Result<Vec<String>> {
        let response = self
            .admin_gateway
            .request(ListTablesRequest::new(database_name)?)
            .await?;
        Ok(response.table_name)
    }
}