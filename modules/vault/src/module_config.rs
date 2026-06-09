use crate::scopes::Scope;

pub trait ModuleConfig: serde::de::DeserializeOwned + serde::Serialize + Default {
    const SCOPE: Scope;
    const CONFIG_NAME: &'static str;
}