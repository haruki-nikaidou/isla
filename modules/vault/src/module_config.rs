use crate::scopes::Scope;

pub trait ModuleConfig: serde::de::DeserializeOwned + serde::Serialize + Default {
    const SCOPE: Scope;
    const CONFIG_KEY: &'static str;
}