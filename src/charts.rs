use anyhow::{Context, Result};
use plotters::prelude::*;
use std::path::Path;

pub const COLOR_BOOKS: RGBColor = RGBColor(31, 119, 180);
pub const COLOR_AUTHORS: RGBColor = RGBColor(44, 160, 44);
pub const COLOR_PUBLISHERS: RGBColor = RGBColor(255, 127, 14);

pub fn cumulative(values: &[u64]) -> Vec<u64> {
    let mut total = 0_u64;
    values
        .iter()
        .map(|value| {
            total = total.saturating_add(*value);
            total
        })
        .collect()
}

pub fn simplify_x_labels(labels: &[String], max_labels: usize) -> Vec<String> {
    if labels.len() <= max_labels || max_labels == 0 {
        return labels.to_vec();
    }
    let step = ((labels.len() as f64) / (max_labels as f64)).ceil() as usize;
    labels
        .iter()
        .enumerate()
        .map(|(idx, label)| {
            if idx % step == 0 || idx + 1 == labels.len() {
                label.clone()
            } else {
                String::new()
            }
        })
        .collect()
}

pub fn draw_totals_chart(path: &Path, labels: &[String], values: &[u64]) -> Result<()> {
    let root = BitMapBackend::new(path, (1280, 560)).into_drawing_area();
    root.fill(&WHITE)?;

    let max_value = values.iter().copied().max().unwrap_or(1).max(1);
    let upper = ((max_value as f64) * 1.20).ceil() as u64 + 1;
    let display_labels = simplify_x_labels(labels, labels.len());
    let x_count = labels.len() as i32;
    let mesh_labels = display_labels.clone();

    let mut chart = ChartBuilder::on(&root)
        .margin(24)
        .caption("Totals (Distinct)", ("sans-serif", 30).into_font())
        .x_label_area_size(60)
        .y_label_area_size(70)
        .build_cartesian_2d(0i32..x_count, 0u64..upper)
        .context("failed to build totals chart")?;

    chart
        .configure_mesh()
        .disable_mesh()
        .x_labels(labels.len())
        .y_desc("Count")
        .x_label_formatter(&move |x| {
            let idx = (*x).max(0) as usize;
            mesh_labels.get(idx).cloned().unwrap_or_default()
        })
        .draw()?;

    let colors = [COLOR_BOOKS, COLOR_AUTHORS, COLOR_PUBLISHERS];
    chart.draw_series(values.iter().enumerate().map(|(idx, value)| {
        Rectangle::new(
            [(idx as i32, 0), (idx as i32 + 1, *value)],
            colors[idx % colors.len()].filled(),
        )
    }))?;

    root.present()?;
    Ok(())
}

pub fn draw_bar_with_cumulative(
    path: &Path,
    title: &str,
    labels: &[String],
    values: &[u64],
    color: RGBColor,
    max_labels: usize,
) -> Result<()> {
    let root = BitMapBackend::new(path, (1280, 560)).into_drawing_area();
    root.fill(&WHITE)?;

    let display_labels = simplify_x_labels(labels, max_labels.max(1));
    let cum_values = cumulative(values);
    let max_new = values.iter().copied().max().unwrap_or(1).max(1);
    let max_cum = cum_values.last().copied().unwrap_or(1).max(1);
    let upper_new = ((max_new as f64) * 1.15).ceil() as u64 + 1;
    let upper_cum = ((max_cum as f64) * 1.10).ceil() as u64 + 1;
    let x_count = labels.len() as i32;

    let mesh_labels_primary = display_labels.clone();
    let mut chart = ChartBuilder::on(&root)
        .margin(24)
        .caption(title, ("sans-serif", 28).into_font())
        .x_label_area_size(70)
        .y_label_area_size(70)
        .right_y_label_area_size(70)
        .build_cartesian_2d(0i32..x_count, 0u64..upper_new)
        .context("failed to build bar/cumulative chart")?
        .set_secondary_coord(0i32..x_count, 0u64..upper_cum);

    chart
        .configure_mesh()
        .disable_mesh()
        .x_labels(max_labels.max(2))
        .y_desc("New Inflow")
        .x_label_formatter(&move |x| {
            let idx = (*x).max(0) as usize;
            mesh_labels_primary.get(idx).cloned().unwrap_or_default()
        })
        .draw()?;

    chart.configure_secondary_axes().y_desc("Cumulative").draw()?;

    chart.draw_series(values.iter().enumerate().map(|(idx, value)| {
        Rectangle::new(
            [(idx as i32, 0), (idx as i32 + 1, *value)],
            color.mix(0.55).filled(),
        )
    }))?;

    chart.draw_secondary_series(LineSeries::new(
        cum_values
            .iter()
            .enumerate()
            .map(|(idx, value)| (idx as i32, *value)),
        &color,
    ))?;

    root.present()?;
    Ok(())
}
