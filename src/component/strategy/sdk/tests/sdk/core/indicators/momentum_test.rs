use super::*;

#[test]
fn rsi_requires_the_complete_warmup_window() {
    let prices = vec![44.0, 44.34, 44.09, 44.15, 43.61, 44.33, 44.83, 45.85, 46.08, 45.89];
    assert!(rsi_batch(prices, 14).unwrap().is_empty());
    assert_eq!(rsi_batch((1..=15).map(f64::from).collect(), 14).unwrap(), vec![100.0]);
    assert_eq!(
        rsi_batch((1..=15).rev().map(f64::from).collect(), 14).unwrap(),
        vec![0.0]
    );
}

#[test]
fn rsi_uses_wilders_gain_and_loss_averages() {
    let result = rsi_batch(vec![10.0, 11.0, 12.0, 11.0, 10.0], 3).unwrap();
    assert_eq!(result.len(), 2);
    assert!((result[0] - 200.0 / 3.0).abs() < 1e-10);
    assert!((result[1] - 400.0 / 9.0).abs() < 1e-10);
}
