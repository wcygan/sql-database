use std::cmp::Ordering;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SqlType {
    Int,
    Text,
    Bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum Value {
    Int(i64),
    Text(String),
    Bool(bool),
    Null,
}

impl Value {
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }

    pub fn cmp_same_type(&self, other: &Value) -> Option<Ordering> {
        match (self, other) {
            (Value::Int(a), Value::Int(b)) => Some(a.cmp(b)),
            (Value::Text(a), Value::Text(b)) => Some(a.cmp(b)),
            (Value::Bool(a), Value::Bool(b)) => Some(a.cmp(b)),
            _ => None,
        }
    }

    pub fn eq_same_type(&self, other: &Value) -> Option<bool> {
        match (self, other) {
            (Value::Int(a), Value::Int(b)) => Some(a.eq(b)),
            (Value::Text(a), Value::Text(b)) => Some(a.eq(b)),
            (Value::Bool(a), Value::Bool(b)) => Some(a.eq(b)),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::cmp::Ordering::{Equal, Greater, Less};

    #[test]
    fn cmp_same_type_works() {
        assert_eq!(Value::Int(1).cmp_same_type(&Value::Int(2)), Some(Less));
        assert_eq!(Value::Int(1).cmp_same_type(&Value::Text("1".into())), None);
    }

    #[test]
    fn truthiness_is_strict() {
        assert_eq!(Value::Bool(true).as_bool(), Some(true));
        assert_eq!(Value::Bool(false).as_bool(), Some(false));
        assert_eq!(Value::Int(1).as_bool(), None);
        assert_eq!(Value::Text("true".into()).as_bool(), None);
        assert_eq!(Value::Null.as_bool(), None);
    }

    #[test]
    fn comparisons_require_same_type() {
        assert_eq!(Value::Int(1).cmp_same_type(&Value::Int(2)), Some(Less));
        assert_eq!(
            Value::Text("a".into()).cmp_same_type(&Value::Text("a".into())),
            Some(Equal)
        );
        assert_eq!(
            Value::Bool(true).cmp_same_type(&Value::Bool(false)),
            Some(Greater)
        );
        // Cross-type should reject
        assert_eq!(Value::Int(1).cmp_same_type(&Value::Text("1".into())), None);
        assert_eq!(Value::Null.cmp_same_type(&Value::Int(1)), None);
    }

    #[test]
    fn equality_requires_same_type() {
        assert_eq!(Value::Int(1).eq_same_type(&Value::Int(1)), Some(true));
        assert_eq!(Value::Int(1).eq_same_type(&Value::Int(2)), Some(false));
        assert_eq!(
            Value::Text("abc".into()).eq_same_type(&Value::Text("abc".into())),
            Some(true)
        );
        assert_eq!(
            Value::Bool(true).eq_same_type(&Value::Bool(false)),
            Some(false)
        );
        // Cross-type returns None
        assert_eq!(Value::Text("1".into()).eq_same_type(&Value::Int(1)), None);
    }

    #[test]
    fn serde_round_trip_stability() {
        let vals = vec![
            Value::Int(-42),
            Value::Text("Ada".into()),
            Value::Bool(true),
            Value::Null,
        ];

        let json = serde_json::to_string(&vals).unwrap();
        let back: Vec<Value> = serde_json::from_str(&json).unwrap();

        assert_eq!(vals, back);
    }

    #[test]
    fn ordering_is_consistent() {
        let a = Value::Int(5);
        let b = Value::Int(7);
        let c = Value::Int(5);

        assert_eq!(a.cmp_same_type(&b), Some(Less));
        assert_eq!(b.cmp_same_type(&a), Some(Greater));
        assert_eq!(a.cmp_same_type(&c), Some(Equal));
    }

    proptest! {
        // Order symmetry: if a < b, then b > a
        #[test]
        fn order_is_antisymmetric(i in any::<i64>(), j in any::<i64>()) {
            let a = Value::Int(i);
            let b = Value::Int(j);
            let ord1 = a.cmp_same_type(&b);
            let ord2 = b.cmp_same_type(&a);
            match (ord1, ord2) {
                (Some(o1), Some(o2)) => assert_eq!(o1, o2.reverse()),
                _ => prop_assert!(true),
            }
        }

        // Reflexivity: every value equals itself
        #[test]
        fn eq_reflexive(val in any::<i64>()) {
            let v = Value::Int(val);
            assert_eq!(v.eq_same_type(&v), Some(true));
        }

        // Text comparisons align with standard String ordering
        #[test]
        fn text_cmp_matches_std(a in ".*", b in ".*") {
            let va = Value::Text(a.clone());
            let vb = Value::Text(b.clone());
            assert_eq!(va.cmp_same_type(&vb), Some(a.cmp(&b)));
        }
    }
}
