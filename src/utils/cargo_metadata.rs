use crate::error::Result;
use failure::ResultExt;
use rustwide::{cmd::Command, Toolchain, Workspace};
use serde::{Deserialize, Serialize};
use std::path::Path;

pub(crate) struct CargoMetadata {
    root: ::cargo_metadata::Package,
}

impl CargoMetadata {
    pub(crate) fn load(
        workspace: &Workspace,
        toolchain: &Toolchain,
        source_dir: &Path,
    ) -> Result<Self> {
        let res = Command::new(workspace, toolchain.cargo())
            .args(&["metadata", "--format-version", "1"])
            .cd(source_dir)
            .log_output(false)
            .run_capture()?;

        let metadata = cargo_metadata::MetadataCommand::parse(&res.stdout_lines().join("\n"))
            .context("invalid output returned by `cargo metadata`")?;
        let resolve = metadata
            .resolve
            .ok_or(failure::err_msg("expected resolve metadata"))?;
        let root = metadata.resolve.root;

        Ok(CargoMetadata {
            root: metadata
                .packages
                .into_iter()
                .find(|pkg| pkg.id == root)
                .unwrap(),
        })
    }

    pub(crate) fn root(&self) -> &::cargo_metadata::Package {
        &self.root
    }
}

/*
#[derive(Deserialize, Serialize)]
pub(crate) struct Package {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) version: String,
    pub(crate) license: Option<String>,
    pub(crate) repository: Option<String>,
    pub(crate) homepage: Option<String>,
    pub(crate) description: Option<String>,
    pub(crate) documentation: Option<String>,
    pub(crate) dependencies: Vec<Dependency>,
    pub(crate) targets: Vec<Target>,
    pub(crate) readme: Option<String>,
    pub(crate) keywords: Vec<String>,
    pub(crate) authors: Vec<String>,
    pub(crate) features: HashMap<String, Vec<String>>,
}
*/

pub(crate) use cargo_metadata::{Metadata, Package};

pub(crate) trait PackageExt {
    fn library_target(&self) -> Option<&Target>;
    fn is_library(&self) -> bool;
    fn normalize_package_name(&self, name: &str) -> String;
    fn package_name(&self) -> String;
    fn library_name(&self) -> Option<String>;
}

impl PackageExt for Package {
    fn library_target(&self) -> Option<&Target> {
        self.targets
            .iter()
            .find(|target| target.crate_types.iter().any(|kind| kind != "bin"))
    }

    fn is_library(&self) -> bool {
        self.library_target().is_some()
    }

    fn normalize_package_name(&self, name: &str) -> String {
        name.replace('-', "_")
    }

    fn package_name(&self) -> String {
        self.library_name()
            .unwrap_or_else(|| self.normalize_package_name(&self.targets[0].name))
    }

    fn library_name(&self) -> Option<String> {
        self.library_target()
            .map(|target| self.normalize_package_name(&target.name))
    }
}

#[derive(Deserialize, Serialize)]
pub(crate) struct Target {
    pub(crate) name: String,
    #[cfg(not(test))]
    crate_types: Vec<String>,
    #[cfg(test)]
    pub(crate) crate_types: Vec<String>,
    pub(crate) src_path: Option<String>,
}

impl Target {
    #[cfg(test)]
    pub(crate) fn dummy_lib(name: String, src_path: Option<String>) -> Self {
        Target {
            name,
            crate_types: vec!["lib".into()],
            src_path,
        }
    }
}

#[derive(Deserialize, Serialize)]
pub(crate) struct Dependency {
    pub(crate) name: String,
    pub(crate) req: String,
    pub(crate) kind: Option<String>,
    pub(crate) rename: Option<String>,
    pub(crate) optional: bool,
}

#[derive(Deserialize, Serialize)]
struct DeserializedMetadata {
    packages: Vec<Package>,
    resolve: DeserializedResolve,
}

#[derive(Deserialize, Serialize)]
struct DeserializedResolve {
    root: String,
    nodes: Vec<DeserializedResolveNode>,
}

#[derive(Deserialize, Serialize)]
struct DeserializedResolveNode {
    id: String,
    deps: Vec<DeserializedResolveDep>,
}

#[derive(Deserialize, Serialize)]
struct DeserializedResolveDep {
    pkg: String,
}
