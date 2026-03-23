#[derive(Clone)]
pub enum QueryPlans {
    Explain { ir: String, phys: String },
    Dot { ir: String, phys: String },
}

impl QueryPlans {
    pub fn size(&self) -> u64 {
        match self {
            Self::Explain { ir, phys } => ir.len() as u64 + phys.len() as u64,
            Self::Dot { ir, phys } => ir.len() as u64 + phys.len() as u64,
        }
    }

    pub fn phys_explain(&self) -> Option<&str> {
        match self {
            QueryPlans::Explain { phys, .. } => Some(phys),
            QueryPlans::Dot { .. } => None,
        }
    }

    pub fn phys_dot(&self) -> Option<&str> {
        match self {
            QueryPlans::Explain { .. } => None,
            QueryPlans::Dot { phys, .. } => Some(phys),
        }
    }
}
