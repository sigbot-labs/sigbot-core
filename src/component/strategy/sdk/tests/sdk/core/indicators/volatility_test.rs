use super::*;

#[test]
fn test_bollinger_bands_batch() {
    let prices = vec![100.0, 102.0, 101.0, 103.0, 105.0, 104.0, 106.0];
    let (upper, middle, lower) = bollinger_bands_batch(prices, 3, 2.0).unwrap();

    assert_eq!(upper.len(), 5);
    assert_eq!(middle.len(), 5);
    assert_eq!(lower.len(), 5);

    // Middle band should be SMA
    assert!((middle[0] - (100.0 + 102.0 + 101.0) / 3.0).abs() < 1e-10);
}

#[test]
fn test_atr_batch() {
    let highs = vec![105.0, 107.0, 106.0, 108.0, 110.0];
    let lows = vec![100.0, 102.0, 101.0, 103.0, 105.0];
    let closes = vec![103.0, 105.0, 104.0, 107.0, 109.0];

    let result = atr_batch(highs, lows, closes, 3).unwrap();
    assert!(!result.is_empty());
    for &x in &result {
        assert!(x > 0.0);
    }
}
