import pandas as pd
from prophet import Prophet
import yaml
import os

def load_config(config_path='config/config.yaml'):
    """Loads the YAML configuration file."""
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

def load_and_prepare_data(file_path):
    """
    Loads the CLEANED data and prepares it for Prophet.
    The data is already in a long format, so we just aggregate by day.
    """
    print(f"Loading cleaned data from {file_path}...")
    df = pd.read_csv(file_path)
    df['date'] = pd.to_datetime(df['date'])

    # Aggregate the now-clean data by day to get total sales
    daily_summary = df.groupby('date').agg(
        total_sales=('total_sales', 'first') # 'first' is fine since it's the same for each day
    ).reset_index()

    # Prophet requires 'ds' and 'y'
    prophet_df = daily_summary[['date', 'total_sales']].rename(columns={
        'date': 'ds',
        'total_sales': 'y'
    })
    
    print("Data preparation for Prophet complete.")
    return prophet_df

def train_and_forecast(df, periods):
    """Trains the Prophet model and makes a future prediction."""
    print("Training Prophet model...")
    model = Prophet(daily_seasonality=False, weekly_seasonality=True, yearly_seasonality=False)
    model.fit(df)
    
    print(f"Making forecast for the next {periods} days...")
    future = model.make_future_dataframe(periods=periods)
    forecast = model.predict(future)
    
    print("Forecasting complete.")
    return model, forecast

def save_results(model, forecast, config):
    """Saves the forecast data and plots to the output directory."""
    output_dir = config['output_path']
    os.makedirs(output_dir, exist_ok=True)

    forecast_path = os.path.join(output_dir, 'sales_forecast.csv')
    forecast.to_csv(forecast_path, index=False)
    print(f"Forecast data saved to {forecast_path}")

    plot_path = os.path.join(output_dir, 'sales_forecast_plot.png')
    fig1 = model.plot(forecast)
    fig1.suptitle(config['forecasting']['plot_title'])
    fig1.gca().set_xlabel(config['forecasting']['plot_xlabel'])
    fig1.gca().set_ylabel(config['forecasting']['plot_ylabel'])
    fig1.savefig(plot_path)
    print(f"Forecast plot saved to {plot_path}")

    components_plot_path = os.path.join(output_dir, 'sales_forecast_components.png')
    fig2 = model.plot_components(forecast)
    fig2.savefig(components_plot_path)
    print(f"Components plot saved to {components_plot_path}")

def main():
    """Main function to run the forecasting pipeline."""
    print("\nStarting Sales Forecasting Module")
    config = load_config()
    
    # IMPORTANT: Update the config to point to the PROCESSED file
    cleaned_data_path = os.path.join(config['processed_data_path'], 'marketing_summary_cleaned.csv')
    
    prophet_data = load_and_prepare_data(cleaned_data_path)
    
    model, forecast = train_and_forecast(prophet_data, config['forecasting']['prediction_periods'])
    
    save_results(model, forecast, config)
    
    print("Forecasting module finished successfully")

if __name__ == '__main__':
    main()