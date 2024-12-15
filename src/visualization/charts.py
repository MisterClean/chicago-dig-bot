"""Module for generating visualizations of Chicago 811 dig ticket data."""
from typing import Tuple, Optional
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import folium
from folium.plugins import HeatMap
from datetime import datetime, timedelta
from pathlib import Path
import matplotlib.dates as mdates
from config import config
from utils.logging import get_logger
import json
import osmnx as ox
import geopandas as gpd
from shapely.geometry import shape
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import os
from PIL import Image
import io

logger = get_logger(__name__)

class ChartGenerationError(Exception):
    """Custom exception for chart generation errors."""
    pass

class ChartGenerator:
    """Handles generation of data visualizations."""
    
    def __init__(self):
        """Initialize chart generator with style settings."""
        self.style = config.chart_style
        self.colors = config.chart_colors
        self.max_image_size = 900 * 1024  # 900KB target size (to be safe under 976.56KB limit)
        
        # Get Chicago boundary using OSMnx with a more reliable method
        try:
            # Use place name query which is more reliable
            gdf = ox.geocode_to_gdf('Chicago, Illinois, USA')
            
            # Convert to GeoJSON with proper coordinate order
            geojson = json.loads(gdf.to_json())
            # Extract the first feature's geometry (Chicago's boundary)
            geometry = geojson['features'][0]['geometry']
            
            # Create the feature with proper structure
            self.chicago_bounds = {
                'type': 'Feature',
                'geometry': geometry
            }
            logger.debug("Successfully loaded Chicago boundary data")
        except Exception as e:
            logger.error(f"Failed to load Chicago boundary data: {str(e)}")
            # Fallback to rectangular bounds if city boundary fails to load
            self.chicago_bounds = {
                "type": "Feature",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[
                        [-87.940267, 42.023131],
                        [-87.523661, 42.023131],
                        [-87.523661, 41.644286],
                        [-87.940267, 41.644286],
                        [-87.940267, 42.023131]
                    ]]
                }
            }
        logger.debug(f"Initialized ChartGenerator with style: {self.style}")

    def _compress_image(self, input_path: str, output_path: str) -> None:
        """Compress image to meet size requirements.
        
        Args:
            input_path: Path to the input image file.
            output_path: Path to save the compressed image.
            
        Raises:
            ChartGenerationError: If compression fails.
        """
        try:
            # Open the image
            with Image.open(input_path) as img:
                # Convert to RGB if necessary
                if img.mode in ('RGBA', 'P'):
                    img = img.convert('RGB')
                
                # Start with quality 95
                quality = 95
                min_quality = 60  # Don't go below this quality
                
                while quality >= min_quality:
                    # Save with current quality
                    temp_buffer = io.BytesIO()
                    img.save(temp_buffer, format='JPEG', quality=quality, optimize=True)
                    size = temp_buffer.tell()
                    
                    if size <= self.max_image_size:
                        # Save the compressed image
                        img.save(output_path, format='JPEG', quality=quality, optimize=True)
                        logger.info(f"Compressed image to {size/1024:.1f}KB with quality {quality}")
                        return
                    
                    # Reduce quality and try again
                    quality -= 5
                
                # If we get here, even lowest quality is too big
                # Try reducing dimensions
                while img.size[0] > 800:
                    new_size = (int(img.size[0] * 0.8), int(img.size[1] * 0.8))
                    img = img.resize(new_size, Image.Resampling.LANCZOS)
                    
                    # Try saving with original quality first
                    quality = 95
                    while quality >= min_quality:
                        temp_buffer = io.BytesIO()
                        img.save(temp_buffer, format='JPEG', quality=quality, optimize=True)
                        size = temp_buffer.tell()
                        
                        if size <= self.max_image_size:
                            img.save(output_path, format='JPEG', quality=quality, optimize=True)
                            logger.info(f"Compressed image to {size/1024:.1f}KB with size {img.size} and quality {quality}")
                            return
                        
                        quality -= 5
                
                raise ChartGenerationError("Could not compress image to meet size requirements")
                
        except Exception as e:
            error_msg = f"Failed to compress image: {str(e)}"
            logger.error(error_msg)
            raise ChartGenerationError(error_msg)

    def _validate_data(self, df: pd.DataFrame) -> None:
        """Validate input data for chart generation."""
        required_columns = ['date', 'regular_tickets', 'emergency_tickets']
        
        if df.empty:
            raise ChartGenerationError("Cannot generate chart from empty DataFrame")
            
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            raise ChartGenerationError(f"Missing required columns: {missing_cols}")
            
        if not pd.api.types.is_datetime64_any_dtype(df['date']):
            raise ChartGenerationError("'date' column must be datetime type")
            
        for col in ['regular_tickets', 'emergency_tickets']:
            if not pd.api.types.is_numeric_dtype(df[col]):
                raise ChartGenerationError(f"'{col}' must contain numeric data")
                
    def _validate_location_data(self, df: pd.DataFrame) -> None:
        """Validate location data for heatmap generation."""
        required_columns = ['latitude', 'longitude', 'dig_date']
        
        if df.empty:
            raise ChartGenerationError("Cannot generate heatmap from empty DataFrame")
            
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            raise ChartGenerationError(f"Missing required columns: {missing_cols}")
            
        for col in ['latitude', 'longitude']:
            if not pd.api.types.is_numeric_dtype(df[col]):
                raise ChartGenerationError(f"'{col}' must contain numeric data")

    def _setup_plot(self) -> None:
        """Configure plot settings and style."""
        try:
            plt.style.use('seaborn-v0_8')
            sns.set_style("whitegrid", {
                'grid.linestyle': ':',
                'grid.color': '#E0E0E0',
                'axes.facecolor': '#F8F8F8'
            })
            
            plt.figure(figsize=self.style['figure_size'], dpi=self.style['dpi'])
            logger.debug(f"Created figure with size {self.style['figure_size']} and DPI {self.style['dpi']}")
                
        except Exception as e:
            error_msg = f"Failed to setup plot: {str(e)}"
            logger.error(error_msg)
            raise ChartGenerationError(error_msg)

    def _capture_map_screenshot(self, html_path: str, output_path: str) -> None:
        """Capture a screenshot of the HTML map using Selenium."""
        try:
            # Setup Chrome options for headless mode
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--window-size=900,900")
            chrome_options.add_argument("--hide-scrollbars")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            
            # Initialize driver
            driver = webdriver.Chrome(options=chrome_options)
            
            # Set viewport size
            driver.set_window_size(900, 900)
            
            # Get absolute path to HTML file
            abs_path = os.path.abspath(html_path)
            file_url = f"file://{abs_path}"
            
            # Load the HTML file
            driver.get(file_url)
            
            # Wait for map to render
            time.sleep(5)
            
            try:
                WebDriverWait(driver, 10).until(
                    lambda d: d.execute_script(
                        "return document.readyState === 'complete' && "
                        "document.querySelector('.leaflet-tile-loaded') !== null"
                    )
                )
            except Exception as e:
                logger.warning(f"Timeout waiting for map tiles: {e}")
            
            time.sleep(2)
            
            # Take screenshot to temporary file
            temp_path = output_path + '.temp.png'
            driver.save_screenshot(temp_path)
            
            # Close driver
            driver.quit()
            
            # Compress the screenshot
            self._compress_image(temp_path, output_path)
            
            # Remove temporary file
            Path(temp_path).unlink()
            
            logger.info(f"Successfully captured and compressed screenshot to {output_path}")
            
        except Exception as e:
            error_msg = f"Failed to capture map screenshot: {str(e)}"
            logger.error(error_msg)
            raise ChartGenerationError(error_msg)

    def create_heatmap(self, df: pd.DataFrame, output_path: str) -> str:
        """Create a heatmap visualization of dig ticket locations."""
        try:
            logger.info("Starting heatmap generation")
            
            # Validate location data
            self._validate_location_data(df)
            
            # Filter for permits from yesterday only
            yesterday = datetime.now().date() - timedelta(days=1)
            df = df[df['dig_date'].dt.date == yesterday]
            
            # Filter out invalid coordinates
            df = df[
                (df['latitude'].between(41.5, 42.5)) &  # Valid Chicago latitude range
                (df['longitude'].between(-88, -87)) &   # Valid Chicago longitude range
                (df['latitude'] != 0) &                 # Remove 0 values
                (df['longitude'] != 0)                  # Remove 0 values
            ]
            
            if df.empty:
                raise ChartGenerationError("No valid location data after filtering")
            
            logger.info(f"Processing {len(df)} permits with valid coordinates")
            
            # Create base map centered on Chicago with no zoom controls
            m = folium.Map(
                location=[41.7866, -87.6818],  # Adjusted center coordinates south
                zoom_start=10.25,  # Decreased zoom level to show more area
                tiles='cartodbpositron',
                width=900,
                height=900,
                zoomControl=False  # Remove zoom controls
            )
            
            # Add Chicago boundary
            folium.GeoJson(
                self.chicago_bounds,
                style_function=lambda x: {
                    'color': '#404040',
                    'weight': 1.5,
                    'fillOpacity': 0,
                }
            ).add_to(m)
            
            # Prepare heatmap data
            locations = df[['latitude', 'longitude']].values.tolist()
            
            # Add heatmap layer with monochromatic blue gradient
            HeatMap(
                locations,
                radius=15,
                blur=20,
                max_zoom=13,
                min_opacity=0.4,
                gradient={
                    '0.4': '#E3F2FD',  # Lightest blue
                    '0.6': '#64B5F6',  # Light blue
                    '0.8': '#1E88E5',  # Medium blue
                    '1.0': '#0D47A1'   # Dark blue
                }
            ).add_to(m)
            
            # Save map to temporary HTML file
            temp_html = output_path + '.temp.html'
            m.save(temp_html)
            
            # Capture screenshot and save as compressed image
            self._capture_map_screenshot(temp_html, output_path)
            
            # Remove temporary HTML file
            Path(temp_html).unlink()
            
            logger.info(f"Saved heatmap to {output_path}")
            
            return output_path
            
        except Exception as e:
            error_msg = f"Failed to generate heatmap: {str(e)}"
            logger.error(error_msg)
            raise ChartGenerationError(error_msg)

    def create_emergency_heatmap(self, df: pd.DataFrame, output_path: str) -> str:
        """Create a heatmap visualization distinguishing emergency vs regular dig tickets."""
        try:
            logger.info("Starting emergency heatmap generation")
            
            # Validate location data
            self._validate_location_data(df)
            if 'is_emergency' not in df.columns:
                raise ChartGenerationError("Missing 'is_emergency' column")
            
            # Filter for permits from yesterday only
            yesterday = datetime.now().date() - timedelta(days=1)
            df = df[df['dig_date'].dt.date == yesterday]
            
            # Filter out invalid coordinates
            df = df[
                (df['latitude'].between(41.5, 42.5)) &  # Valid Chicago latitude range
                (df['longitude'].between(-88, -87)) &   # Valid Chicago longitude range
                (df['latitude'] != 0) &                 # Remove 0 values
                (df['longitude'] != 0)                  # Remove 0 values
            ]
            
            if df.empty:
                raise ChartGenerationError("No valid location data after filtering")
            
            logger.info(f"Processing {len(df)} permits with valid coordinates")
            
            # Create base map centered on Chicago with no zoom controls
            m = folium.Map(
                location=[41.7866, -87.6818],  # Adjusted center coordinates south
                zoom_start=10.71,  # Decreased zoom level to show more area
                tiles='cartodbpositron',
                width=950,
                height=950,
                zoomControl=False  # Remove zoom controls
            )
            
            # Add Chicago boundary
            folium.GeoJson(
                self.chicago_bounds,
                style_function=lambda x: {
                    'color': '#404040',
                    'weight': 1.5,
                    'fillOpacity': 0,
                }
            ).add_to(m)
            
            # Convert emergency column to boolean if it's not already
            df = df.copy()
            df['is_emergency'] = df['is_emergency'].astype(bool)
            
            # Split data into emergency and regular
            emergency_df = df[df['is_emergency']]
            regular_df = df[~df['is_emergency']]
            
            # Add regular tickets heatmap layer with monochromatic blue gradient
            regular_locations = regular_df[['latitude', 'longitude']].values.tolist()
            if regular_locations:
                regular_heatmap = HeatMap(
                    regular_locations,
                    name="Regular Permits",
                    radius=15,
                    blur=20,
                    max_zoom=13,
                    min_opacity=0.4,
                    gradient={
                        '0.4': '#E3F2FD',  # Lightest blue
                        '0.6': '#64B5F6',  # Light blue
                        '0.8': '#1E88E5',  # Medium blue
                        '1.0': '#0D47A1'   # Dark blue
                    }
                )
                regular_heatmap.add_to(m)
            
            # Add emergency tickets heatmap layer with monochromatic red gradient
            emergency_locations = emergency_df[['latitude', 'longitude']].values.tolist()
            if emergency_locations:
                emergency_heatmap = HeatMap(
                    emergency_locations,
                    name="Emergency Permits",
                    radius=15,
                    blur=20,
                    max_zoom=13,
                    min_opacity=0.4,
                    gradient={
                        '0.4': '#FFEBEE',  # Lightest red
                        '0.6': '#EF5350',  # Light red
                        '0.8': '#E53935',  # Medium red
                        '1.0': '#B71C1C'   # Dark red
                    }
                )
                emergency_heatmap.add_to(m)
            
            # Add legend in top right with improved visibility
            legend_html = '''
                <div style="position: fixed; 
                            top: 20px; right: 20px;
                            border: 2px solid rgba(0,0,0,0.2);
                            background-color: white;
                            padding: 10px;
                            border-radius: 6px;
                            box-shadow: 0 2px 4px rgba(0,0,0,0.2);
                            font-size: 14px;
                            font-weight: bold;">
                    <p style="margin: 0; padding: 5px;">
                        <span style="display: inline-block; width: 16px; height: 16px; background: linear-gradient(to right, #E3F2FD, #0D47A1); margin-right: 8px; vertical-align: middle;"></span>
                        Regular Permits
                    </p>
                    <p style="margin: 0; padding: 5px;">
                        <span style="display: inline-block; width: 16px; height: 16px; background: linear-gradient(to right, #FFEBEE, #B71C1C); margin-right: 8px; vertical-align: middle;"></span>
                        Emergency Permits
                    </p>
                </div>
            '''
            m.get_root().html.add_child(folium.Element(legend_html))
            
            # Save map to temporary HTML file
            temp_html = output_path + '.temp.html'
            m.save(temp_html)
            
            # Capture screenshot and save as compressed image
            self._capture_map_screenshot(temp_html, output_path)
            
            # Remove temporary HTML file
            Path(temp_html).unlink()
            
            logger.info(f"Saved emergency heatmap to {output_path}")
            
            return output_path
            
        except Exception as e:
            error_msg = f"Failed to generate emergency heatmap: {str(e)}"
            logger.error(error_msg)
            raise ChartGenerationError(error_msg)

    def create_daily_chart(self, df: pd.DataFrame) -> Tuple[str, pd.Series]:
        """Create visualization of daily statistics."""
        try:
            logger.info("Starting daily chart generation")
            
            # Validate input data
            logger.debug("Validating input data")
            self._validate_data(df)
            
            # Setup plot
            logger.debug("Setting up plot")
            self._setup_plot()
            
            # Create the main axis
            ax = plt.gca()
            
            # Plot data with enhanced styling
            regular_line = plt.plot(df['date'], df['regular_tickets'],
                    label='Regular Tickets',
                    color=self.colors['regular'],
                    linewidth=2.5,
                    marker='o',
                    markersize=6,
                    markerfacecolor='white',
                    markeredgewidth=1.5,
                    markeredgecolor=self.colors['regular'])
            
            emergency_line = plt.plot(df['date'], df['emergency_tickets'],
                    label='Emergency Tickets',
                    color=self.colors['emergency'],
                    linewidth=2.5,
                    marker='o',
                    markersize=6,
                    markerfacecolor='white',
                    markeredgewidth=1.5,
                    markeredgecolor=self.colors['emergency'])
            
            # Add subtle fill below the lines
            ax.fill_between(df['date'], df['regular_tickets'], alpha=0.1, color=self.colors['regular'])
            ax.fill_between(df['date'], df['emergency_tickets'], alpha=0.1, color=self.colors['emergency'])
            
            # Configure title and labels with enhanced typography
            title = self.style.get('title_format', 'Chicago 811 Dig Tickets - Last {days} Days')
            plt.title(title.format(days=config.soda_days_to_fetch), 
                     pad=20, fontsize=14, fontweight='bold')
            plt.xlabel('Date', fontsize=11, labelpad=10)
            plt.ylabel('Number of Tickets', fontsize=11, labelpad=10)
            
            # Enhance x-axis date formatting
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
            ax.xaxis.set_major_locator(mdates.AutoDateLocator())
            
            # Configure legend with enhanced styling
            legend = plt.legend(bbox_to_anchor=(1.02, 1), 
                              loc='upper left',
                              borderaxespad=0,
                              frameon=True,
                              fancybox=True,
                              shadow=True,
                              fontsize=10)
            
            # Add annotations for the latest values
            latest = df.iloc[-1]
            plt.annotate(f'{int(latest["regular_tickets"])}',
                        xy=(latest['date'], latest['regular_tickets']),
                        xytext=(10, 10), textcoords='offset points',
                        fontsize=9, color=self.colors['regular'],
                        bbox=dict(facecolor='white', edgecolor=self.colors['regular'], alpha=0.7))
            
            plt.annotate(f'{int(latest["emergency_tickets"])}',
                        xy=(latest['date'], latest['emergency_tickets']),
                        xytext=(10, -15), textcoords='offset points',
                        fontsize=9, color=self.colors['emergency'],
                        bbox=dict(facecolor='white', edgecolor=self.colors['emergency'], alpha=0.7))
            
            # Configure layout
            plt.xticks(rotation=30)
            plt.tight_layout()
            
            # Add subtle border
            for spine in ax.spines.values():
                spine.set_edgecolor('#CCCCCC')
                spine.set_linewidth(0.8)
            
            # Save chart with high quality
            chart_path = Path(config.chart_file)
            logger.info(f"Saving chart to {chart_path}")
            plt.savefig(chart_path, bbox_inches='tight', dpi=self.style['dpi'])
            plt.close()
            
            # Get latest stats
            latest_stats = df.iloc[-1]
            logger.info("Chart generation completed successfully")
            logger.debug(f"Latest stats: {latest_stats.to_dict()}")
            
            return str(chart_path), latest_stats
            
        except ChartGenerationError:
            # Re-raise validation errors
            raise
            
        except Exception as e:
            error_msg = f"Failed to generate daily chart: {str(e)}"
            logger.error(error_msg)
            
            # Ensure figure is closed on error
            try:
                plt.close()
            except:
                pass
                
            raise ChartGenerationError(error_msg)
            
    def __del__(self):
        """Cleanup any open matplotlib figures."""
        try:
            plt.close('all')
            logger.debug("Closed all matplotlib figures")
        except Exception as e:
            logger.error(f"Error closing matplotlib figures: {str(e)}")
