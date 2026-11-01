use super::*;

#[test]
fn test_obv_batch() {
    let closes = vec![100.0, 101.0, 100.5, 102.0];
    let volumes = vec![1000.0, 1200.0, 800.0, 1500.0];
    let first_volume = volumes[0];

    let result = obv_batch(closes, volumes).unwrap();
    assert_eq!(result.len(), 4);
    assert_eq!(result[0], first_volume); // First OBV = first volume
}

#[test]
fn test_vwap_batch() {
    let highs = vec![105.0, 107.0, 106.0];
    let lows = vec![100.0, 102.0, 101.0];
    let closes = vec![103.0, 105.0, 104.0];
    let volumes = vec![1000.0, 1200.0, 800.0];

    let result = vwap_batch(highs, lows, closes, volumes).unwrap();
    assert_eq!(result.len(), 3);
    for &x in &result {
        assert!(x > 0.0);
    }
}
