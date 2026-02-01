//! Compaction level definitions

/// Compaction level
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Level {
    /// Level 0: Size-tiered compaction (recent data)
    L0,
    /// Level N: Leveled compaction (older data)
    L(usize),
}

impl Level {
    /// Get the level as a u32
    pub fn as_u32(&self) -> u32 {
        match self {
            Level::L0 => 0,
            Level::L(n) => *n as u32,
        }
    }

    /// Get the level number
    pub fn number(&self) -> usize {
        match self {
            Level::L0 => 0,
            Level::L(n) => *n,
        }
    }

    /// Get the next level
    pub fn next(&self) -> Level {
        match self {
            Level::L0 => Level::L(1),
            Level::L(n) => Level::L(n + 1),
        }
    }
}

impl From<u32> for Level {
    fn from(n: u32) -> Self {
        match n {
            0 => Level::L0,
            n => Level::L(n as usize),
        }
    }
}

impl std::fmt::Display for Level {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Level::L0 => write!(f, "L0"),
            Level::L(n) => write!(f, "L{}", n),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_level_conversion() {
        assert_eq!(Level::L0.as_u32(), 0);
        assert_eq!(Level::L(1).as_u32(), 1);
        assert_eq!(Level::L(3).as_u32(), 3);
    }

    #[test]
    fn test_level_next() {
        assert_eq!(Level::L0.next(), Level::L(1));
        assert_eq!(Level::L(1).next(), Level::L(2));
    }

    #[test]
    fn test_level_from() {
        assert_eq!(Level::from(0), Level::L0);
        assert_eq!(Level::from(2), Level::L(2));
    }
}
