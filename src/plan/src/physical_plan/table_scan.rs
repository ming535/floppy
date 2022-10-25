use catalog::names::FullObjectName;
use common::error::Result;
use common::relation::{GlobalId, RelationDesc, Row};

#[derive(Debug)]
pub struct FullTableScanExec {
    pub table_id: GlobalId,
    pub rel_desc: RelationDesc,
    pub full_name: FullObjectName,
}

#[derive(Debug)]
pub struct PrimaryIndexTableScanExec {
    pub table_id: GlobalId,
    pub rel_desc: RelationDesc,
    pub full_name: FullObjectName,
}

impl PrimaryIndexTableScanExec {
    fn next(&mut self) -> Result<Option<Row>> {
        todo!()
    }
}
