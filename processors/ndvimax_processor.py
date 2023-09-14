from loguru import logger
from simple_settings import settings
from .base_processor import BaseProcessor
import ee
import datetime

class NdviMaxProcessor(BaseProcessor):
    def __init__(self):
        super(NdviMaxProcessor).__init__()

    def run(self):
        """
        Process the NDVI MAX product.

        Returns:
            int: 1 if new imagery is found and processing is performed, 0 otherwise.
        """
        current_date_str = settings.current_date_str
        roi = ee.Geometry.Rectangle(settings.config['ROI_RECTANGLE'])
        product_config = settings.config['PRODUCT_NDVI_MAX']
        logger.info("********* processing {} *********".format(product_config['product_name']))
        current_date = ee.Date(current_date_str)

        # Filter the sensor collection based on date and region
        sensor = (
            ee.ImageCollection(product_config['image_collection'])
            .filterDate(current_date.advance(-int(product_config['temporal_coverage']), 'day'), current_date)
            .filterBounds(roi)
        )

        # Get information about the available sensor data for the range
        sensor_stats = self.get_collection_info(sensor)

        # Check if there is new sensor data compared to the stored dataset
        if self.check_product_update(product_config['product_name'], sensor_stats[1]) is True:
            logger.info("new imagery from: "+sensor_stats[1])

            # Generate the filename
            filename = product_config['prefix']+sensor_stats[0].replace(
                "-", "")+"-"+sensor_stats[1].replace("-", "")+"_run"+current_date_str.replace("-", "")
            logger.info(filename)

            # Create NDVI and NDVI max
            sensor = sensor.map(lambda image: self.addINDEX(
                image, bands=product_config['band_names'][0], index_name="NDVI"))

            mosaic = sensor.qualityMosaic("NDVI")
            ndvi_max = mosaic.select("NDVI")

            # Multiply by 100 to move the decimal point two places back to the left and get rounded values,
            # then round then cast to get int16, Int8 is not a sultion since COGTiff is not supported
            ndvi_max_int = ndvi_max.multiply(100).round().toInt16()

            # Mask outside
            ndvi_max_int = self.maskOutside(ndvi_max_int, roi).unmask(settings.NODATA)

            # Define item Name
            timestamp = datetime.datetime.strptime(current_date_str, '%Y-%m-%d')

            # Check if there is at least 1 scene to be defined (if minimal scene count is required) TODO: is this necessary?
            if sensor_stats[2] > 0:
                # Start the export
                self.prepare_export(roi, timestamp.strftime('%Y%m%dT240000'), filename, product_config['product_name'], product_config['spatial_scale_export'], ndvi_max_int,
                               sensor_stats, current_date_str)

                return 1
            else:
                logger.warning('No candidate scene')
                # TODO: If there are not enough scenes, quit processing
                return 0
        else:
            return "no new imagery"

    def addINDEX(self, image, bands, index_name):
        """
        Add an Index (eg NDVI) band to the image based on two bands.

        Args:
            image (ee.Image): Input image to add the index band.
            bands (dict): Dictionary containing band names for NIR and RED.
            index_name (str): Name of the index used as band name

        Returns:
            ee.Image: Image with the index band added.
        """

        # Extract the band names for NIR and RED from the input dictionary
        NIR = bands['NIR']
        RED = bands['RED']

        # Compute the index using the normalizedDifference() function and rename the band to "NDVI"
        index = image.normalizedDifference([NIR, RED]).rename(index_name)

        # Add the index band to the image using the addBands() function
        image_with_index = image.addBands(index)

        # Return the image with the NDVI band added
        return image_with_index
