use std::sync::Once;

static INIT: Once = Once::new();

pub fn set_up_logging() {
    INIT.call_once(|| {
        color_eyre::install().expect("Failed to install color_eyre.");
    })
}

pub mod macros {

    #[macro_export]
    macro_rules! hmap {
        ($($k:literal: $v:expr),* $(,)?) => {{
            HashMap::from([$(($k, $v),)*])
        }};
    }

    #[macro_export]
    macro_rules! hset {
        ($($k:expr),* $(,)?) => {{
            HashSet::from([$($k,)*])
        }};
    }


    #[cfg(test)]
    pub mod tests {
        use hashbrown::HashMap;
        use hashbrown::HashSet;

        #[test]
        pub fn non_empty_hmap() {
            let map: HashMap<usize, usize> = hmap! {
                1: 2,
                2: 3,
                3: 4usize
            };
            assert_eq!(map, HashMap::from([(1, 2), (2, 3), (3, 4)]));
        }

        #[test]
        pub fn non_empty_hset() {
            let map: HashSet<usize> = hset! {
                1,
                2,
                3
            };
            assert_eq!(map, HashSet::from([1, 2, 3]));
        }

        #[test]
        pub fn empty_hmap() {
            let map: HashMap<usize, usize> = hmap! {};
            assert_eq!(map, HashMap::new());
        }

        #[test]
        pub fn empty_hset() {
            let set: HashSet<usize> = hset! {};
            assert_eq!(set, HashSet::new());
        }
    }
}