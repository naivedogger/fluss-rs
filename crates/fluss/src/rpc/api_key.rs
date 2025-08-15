use crate::rpc::api_key::ApiKey::Unknown;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub enum ApiKey {
    CreateDatabase,
    CreateTable,
    DropTable,
    ListTables,
    ProduceLog,
    FetchLog,
    MetaData,
    GetTable,
    Unknown(i16),
}

impl From<i16> for ApiKey {
    fn from(key: i16) -> Self {
        match key {
            1001 => ApiKey::CreateDatabase,
            1005 => ApiKey::CreateTable,
            1006 => ApiKey::DropTable,
            1008 => ApiKey::ListTables,
            1014 => ApiKey::ProduceLog,
            1015 => ApiKey::FetchLog,
            1012 => ApiKey::MetaData,
            1007 => ApiKey::GetTable,
            _ => Unknown(key),
        }
    }
}

impl From<ApiKey> for i16 {
    fn from(key: ApiKey) -> Self {
        match key {
            ApiKey::CreateDatabase => 1001,
            ApiKey::CreateTable => 1005,
            ApiKey::DropTable => 1006,
            ApiKey::ListTables => 1008,
            ApiKey::ProduceLog => 1014,
            ApiKey::MetaData => 1012,
            ApiKey::GetTable => 1007,
            ApiKey::FetchLog => 1015,
            Unknown(x) => x,
        }
    }
}
