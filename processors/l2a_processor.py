import os
import json
from loguru import logger
from simple_settings import settings
import ee
from .base_processor import BaseProcessor


class L2AProcessor(BaseProcessor):
    def __init__(self):
        super(L2AProcessor).__init__()

    def run(self):
        """
        Export the S2 Level 2A product.

        Returns:
            str: "no new imagery" if no new imagery found, None if new imagery is processed.
        """
        current_date_str = settings.current_date_str
        current_date = ee.Date(current_date_str)
        border = ee.FeatureCollection("USDOS/LSIB_SIMPLE/2017").filter(ee.Filter.eq("country_co", "SZ"))
        roi = border.geometry().buffer(settings.config['ROI_BORDER_BUFFER'])

        # roi = ee.Geometry.Rectangle( [ 7.075402, 46.107098, 7.100894, 46.123639])
        product_config = settings.config['PRODUCT_S2_LEVEL_2A']
        logger.info("********* processing {} *********".format(product_config['product_name']))

        # Filter the sensor collection based on date and region
        collection = (
            ee.ImageCollection(product_config['image_collection'])
            .filterDate(current_date.advance(-int(product_config['temporal_coverage']), 'day'), current_date)
            .filterBounds(roi)
        )

        # Get the number of images found in the collection
        num_images = collection.size().getInfo()

        # Check if there are any new imagery
        if num_images != 0:

            # Get information about the available sensor data for the range
            sensor_stats = self.get_collection_info(collection)

            # Check if there is new sensor data compared to the stored dataset
            if self.check_product_update(product_config['product_name'], sensor_stats[1]) is True:

                # Get the list of images
                image_list = collection.toList(collection.size())
                logger.info(str(image_list.size().getInfo()) + " new image(s) for: " +
                      sensor_stats[1] + " to: " + current_date_str)

                # Generate the mosaic name and snsing date by geeting EE asset ids from the first image
                mosaic_id = ee.Image(image_list.get(0))
                mosaic_id = mosaic_id.id().getInfo()
                mosaic_sensing_timestamp = mosaic_id.split('_')[0]

                # Split the string by underscores
                parts = mosaic_id.split('_')

                # Join the first two parts with an underscore to get the desired result
                mosaic_id = '_'.join(parts[:2])

                # Create a mosaic of the images for the specified date and time
                mosaic = collection.mosaic()

                # Clip Image to ROI
                # might add .unmask(config.NODATA)
                clipped_image = mosaic.clip(roi)

                # Intersect ROI and clipped mosaic
                # Create an empty list to hold the footprints
                footprints = ee.List([])

                # Function to extract footprint from each image and add to the list
                def add_footprint(image, lst):
                    footprint = image.geometry()
                    return ee.List(lst).add(footprint)

                # Map the add_footprint function over the collection to create a list of footprints
                footprints_list = collection.iterate(add_footprint, footprints)

                # Reduce the list of footprints into a single geometry using reduce
                combined_swath_geometry = ee.Geometry.MultiPolygon(footprints_list)

                # Clip the ROI with the combined_swath_geometry
                clipped_roi = roi.intersection(
                    combined_swath_geometry, ee.ErrorMargin(1))

                # Get the bounding box of clippedRoi
                clipped_image_bounding_box = clipped_roi.bounds()

                # Generate the filename
                filename = product_config['prefix'] + \
                    '_' + mosaic_id

                # Export selected bands (B4, B3, B2, B8) as a single GeoTIFF with '_10M'
                multiband_export = clipped_image.select(
                    ['B4', 'B3', 'B2', 'B8'])
                multiband_export_name = filename + '_10M' + \
                    "_run"+current_date_str.replace("-", "")
                self.prepare_export(clipped_image_bounding_box, mosaic_sensing_timestamp, multiband_export_name,
                               product_config['product_name'], product_config['spatial_scale_export'], multiband_export,
                               sensor_stats, current_date_str)

                # Export QA60 band as a separate GeoTIFF with '_QA60'
                qa60_export = clipped_image.select('QA60')
                qa60_export_name = filename + '_QA60' + \
                    "_run"+current_date_str.replace("-", "")
                self.prepare_export(clipped_image_bounding_box, mosaic_sensing_timestamp, qa60_export_name,
                               product_config['product_name'], product_config['spatial_scale_export_qa60'], qa60_export,
                               sensor_stats, current_date_str)

                # For each image, process
                for i in range(image_list.size().getInfo()):
                    image = ee.Image(image_list.get(i))

                    # EE asset ids for Sentinel-2 L2 assets have the following format: 20151128T002653_20151128T102149_T56MNN.
                    #  Here the first numeric part represents the sensing date and time, the second numeric part represents the product generation date and time,
                    #  and the final 6-character string is a unique granule identifier indicating its UTM grid reference
                    image_id = image.id().getInfo()
                    image_sensing_timestamp = image_id.split('_')[0]
                    # first numeric part represents the sensing date, needs to be used in publisher
                    logger.info("processing {} of {} {} ...".
                                format(i+1, image_list.size().getInfo(), image_sensing_timestamp))

                    # Generate the filename
                    filename = product_config['prefix'] + '_' + image_id

                    # Export Image Properties into a json file
                    export_filename = filename + "_properties_run" + current_date_str.replace("-", "") + ".json"
                    with open(os.path.join(settings.processing_dir, export_filename), "w") as json_file:
                        json.dump(image.getInfo(), json_file)

                return 1

            else:
                logger.warning("no new imagery")
                return 0

        else:
            logger.warning("no new imagery")
            return 0

