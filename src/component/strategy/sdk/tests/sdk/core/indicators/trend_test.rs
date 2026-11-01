use super::*;

#[test]
fn test_sma_batch() {
    let prices = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
    let result = sma_batch(prices, 3).unwrap();
    assert_eq!(result.len(), 8);
    assert!((result[0] - 2.0).abs() < 1e-10); // (1+2+3)/3
    assert!((result[1] - 3.0).abs() < 1e-10); // (2+3+4)/3
    assert!((result[7] - 9.0).abs() < 1e-10); // (8+9+10)/3
}

#[test]
fn test_ema_batch() {
    let prices = vec![1.0, 2.0, 3.0, 4.0, 5.0];
    let result = ema_batch(prices, 3).unwrap();
    assert_eq!(result.len(), 5);
    assert!((result[0] - 1.0).abs() < 1e-10); // First value is price itself
}
