from pathlib import Path
from dagster import asset, AssetExecutionContext, MaterializeResult, MetadataValue, AssetKey
from dagster_duckdb import DuckDBResource
import plotly.graph_objects as go
import pandas as pd
import plotly.express as px
from plotly.subplots import make_subplots


@asset(
  deps=[
    AssetKey(["gold_layer", "gold_hourly_metrics"]),
    AssetKey(["gold_layer", "gold_daily_metrics_by_borough"]),
  ],
  group_name="visualization",
  key_prefix=["dashboard"],
)
def nyc_taxi_dashboard(context: AssetExecutionContext, duckdb: DuckDBResource):
  """Generates interactive HTML dashboard with taxi analytics."""

  with duckdb.get_connection() as conn:
    hourly_df = conn.execute("SELECT * FROM main_gold_layer.gold_hourly_metrics ORDER BY hour").fetchdf()
    daily_df = conn.execute("SELECT * FROM main_gold_layer.gold_daily_metrics_by_borough ORDER BY day").fetchdf()
    
    # Parse timestamps - DuckDB returns them as strings, need datetime for Plotly
    hourly_df['hour'] = pd.to_datetime(hourly_df['hour'])
    daily_df['day'] = pd.to_datetime(daily_df['day'])

    # Handle NaN values - some hours might not have weather data
    hourly_df = hourly_df.dropna()
    daily_df = daily_df.dropna()

  # --- Calculations ---
  # 168-hour (7-day) moving average with center=True for symmetric smoothing
  hourly_df['moving_avg'] = hourly_df['total_trips'].rolling(window=168, center=True).mean()
  hourly_df['hour_of_day'] = hourly_df['hour'].dt.hour
  hourly_avg = hourly_df.groupby('hour_of_day')['total_trips'].mean().reset_index()
  
  borough_totals = daily_df.groupby('pickup_borough')['total_trips'].sum().reset_index()
  borough_totals['percentage'] = (borough_totals['total_trips'] / borough_totals['total_trips'].sum() * 100)
  
  # Correlation matrix for weather impact analysis
  corr_df = hourly_df[['total_trips', 'avg_fare', 'avg_distance', 'temperature', 'precipitation']].corr().round(2)


  # --- Chart Creation ---
  fig_hourly = px.line(hourly_df, x='hour', y='total_trips', title='Hourly Trip Volume')
  
  fig_moving = px.line(hourly_df, x='hour', y='moving_avg', title='7-Day Moving Average of Trips')
  fig_moving.update_traces(line_color='orange')
  
  # Divergent color scale centered at 0 (white) for correlation interpretation
  fig_corr = go.Heatmap(
      z=corr_df.values,
      x=corr_df.columns,
      y=corr_df.columns,
      colorscale='RdBu_r',
      zmid=0,
      text=corr_df.values,
      texttemplate="%{text}",
      colorbar=dict(
          title='Correlation',
          len=0.23,
          y=0.5,
          x=0.48,
          yanchor='middle',
          xanchor='left'
      )
  )

  # OLS trendline to quantify temperature-trip correlation
  fig_scatter = px.scatter(
      hourly_df, x="temperature", y="total_trips",
      title="Trip Volume vs. Temperature",
      labels={'temperature': 'Temperature (°C)', 'total_trips': 'Total Trips'},
      trendline="ols",
      trendline_color_override="red"
  )

  # Intraday pattern analysis - shows typical NYC taxi demand cycles
  fig_pattern = px.bar(hourly_avg, x='hour_of_day', y='total_trips', title='Average Trips by Hour of Day')
  fig_pattern.update_traces(marker_color='lightgreen')
  
  # Horizontal bar chart with value labels, sorted by total_trips descending for clearer comparison
  borough_totals_sorted = borough_totals.sort_values('total_trips', ascending=True)
  fig_borough_bars = go.Bar(
      y=borough_totals_sorted.pickup_borough,
      x=borough_totals_sorted.total_trips,
      orientation='h',
      text=[f"{trips:,.0f} ({pct:.1f}%)" for trips, pct in zip(borough_totals_sorted.total_trips, borough_totals_sorted.percentage)],
      textposition='outside',
      marker_color='lightblue',
  )

  # --- HTML Generation ---
  output_path = Path("dashboard.html")
  
  fig = make_subplots(
      rows=3, cols=2,
      subplot_titles=(
          'Hourly Trip Volume', '7-Day Moving Average',
          'Correlation Matrix', 'Trip Volume vs. Temperature',
          'Average Trips by Hour of Day', 'Trip Distribution by Borough'
      ),
          specs=[[{}, {}],
                 [{}, {}],
                 [{}, {}]],
      vertical_spacing=0.15
  )

  fig.add_trace(fig_hourly.data[0], row=1, col=1)
  fig.add_trace(fig_moving.data[0], row=1, col=2)

  fig.add_trace(fig_corr, row=2, col=1)
  
  fig.add_trace(fig_scatter.data[0], row=2, col=2)
  fig.add_trace(fig_scatter.data[1], row=2, col=2)

  fig.add_trace(fig_pattern.data[0], row=3, col=1)
  fig.add_trace(fig_borough_bars, row=3, col=2)

  fig.update_layout(
      height=1200, 
      showlegend=False, 
      title_text="NYC Taxi Analytics Dashboard", 
      title_x=0.5, 
      template='plotly_white'
  )
  
  # Adjust x-axis range for borough chart to prevent label cutoff
  fig.update_xaxes(range=[0, borough_totals_sorted.total_trips.max() * 1.15], row=3, col=2)

  # Use CDN for Plotly.js to keep HTML file size manageable
  html_content = fig.to_html(include_plotlyjs='cdn')
  output_path.write_text(html_content, encoding='utf-8')

  context.log.info(f"Dashboard saved to {output_path.absolute()}")

  return MaterializeResult(
      metadata={
          "dashboard_path": MetadataValue.path(str(output_path.absolute())),
          "total_trips": int(hourly_df['total_trips'].sum()),
          "preview": MetadataValue.md(f"Dashboard with {len(hourly_df)} hourly records.")
      }
  )