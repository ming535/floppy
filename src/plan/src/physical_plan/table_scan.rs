use common::relation::GlobalId;

#[derive(Debug)]
pub struct FullTableScanExec {
    pub table_id: GlobalId,
}

#[derive(Debug)]
pub struct PrimaryIndexTableScanExec {}
