import cv2
import json
import numpy as np
import os
import shutil
from datetime import datetime


class PotholeAreaEstimator:
    def __init__(
        self,
        trapezoid_coords,
        rectangle_coords,
        calibration_path="outputs/camera_calibration.json",
    ):
        """
        Initialize the pothole area estimator.

        Args:
            trapezoid_coords: np.array of shape (4, 2) - detection region coordinates
            rectangle_coords: np.array of shape (4, 2) - BEV rectangle template (in cm)
            calibration_path: Path to camera calibration JSON
        """
        self.rectangle_coords = rectangle_coords
        self.calibration_path = calibration_path

        # Load the normalized trapezoid coordinates
        self.normalized_trapezoid = trapezoid_coords.astype(np.float32)

        # Load camera calibration
        self.camera_matrix, self.dist_coeffs = self.load_camera_calibration()

    def load_camera_calibration(self):
        """Load camera calibration from JSON file"""
        with open(self.calibration_path, "r") as f:
            calib_data = json.load(f)

        camera_matrix = np.array(calib_data["camera_matrix"])
        dist_coeffs = np.array(calib_data["distortion_coefficients"])

        return camera_matrix, dist_coeffs

    def undistort_image_and_coords(self, img_rgb, coords_list):
        """Undistort image and coordinates using camera calibration"""
        h, w = img_rgb.shape[:2]

        # Get optimal new camera matrix
        new_camera_matrix, roi = cv2.getOptimalNewCameraMatrix(
            self.camera_matrix, self.dist_coeffs, (w, h), 1, (w, h)
        )

        # Undistort the image
        undistorted_img = cv2.undistort(
            img_rgb, self.camera_matrix, self.dist_coeffs, None, new_camera_matrix
        )

        # Undistort coordinates
        undistorted_coords = []
        for coords in coords_list:
            coords_reshaped = coords.reshape(-1, 1, 2).astype(np.float32)
            undistorted = cv2.undistortPoints(
                coords_reshaped,
                self.camera_matrix,
                self.dist_coeffs,
                None,
                new_camera_matrix,
            )
            undistorted_coords.append(undistorted.reshape(-1, 2))

        # Handle ROI cropping if needed
        x, y, w_roi, h_roi = roi
        if w_roi > 0 and h_roi > 0:
            undistorted_img = undistorted_img[y : y + h_roi, x : x + w_roi]
            for coords in undistorted_coords:
                coords[:, 0] -= x
                coords[:, 1] -= y

        return undistorted_img, undistorted_coords

    def polygon_area(self, vertices):
        """Calculate polygon area using shoelace formula"""
        vertices = np.array(vertices)
        x = vertices[:, 0]
        y = vertices[:, 1]

        x_shift = np.roll(x, -1)
        y_shift = np.roll(y, -1)

        area = 0.5 * np.abs(np.sum(x * y_shift - y * x_shift))
        return area

    def compute_pothole_area(self, frame_rgb, pothole_mask):
        """
        Run the complete surface area estimation pipeline.
        Uses normalized trapezoid coordinates that adapt to frame resolution.

        Args:
            frame_rgb: Input frame in RGB format
            pothole_mask: Pothole coordinates as np.array of shape (N, 2)

        Returns (area_cm2, bev_image, homography_matrix)
        """
        h, w = frame_rgb.shape[:2]

        # Step 1: Denormalize trapezoid for current frame dimensions
        trapezoid_coords = (self.normalized_trapezoid * np.array([w, h])).astype(
            np.float32
        )

        # Step 2: Undistort
        undistorted_img, undistorted_coords = self.undistort_image_and_coords(
            frame_rgb, [pothole_mask, trapezoid_coords]
        )
        undistorted_pothole = undistorted_coords[0]
        undistorted_trapezoid = undistorted_coords[1]

        # Step 3: Compute homography
        max_w_unit = np.max(self.rectangle_coords[:, 0])
        max_h_unit = np.max(self.rectangle_coords[:, 1])

        pixel_scale = 1  # 1 pixel per cm
        bev_w = int(max_w_unit * pixel_scale)
        bev_h = int(max_h_unit * pixel_scale)

        rectangle_mask_px = (self.rectangle_coords * pixel_scale).astype(np.float32)
        H, _ = cv2.findHomography(
            undistorted_trapezoid.astype(np.float32), rectangle_mask_px
        )

        # Step 4: Generate BEV image with pothole
        bev_img = cv2.warpPerspective(undistorted_img, H, (bev_w, bev_h))

        # Transform pothole to BEV
        pothole_reshaped = undistorted_pothole.reshape(-1, 1, 2).astype(np.float32)
        pothole_bev = cv2.perspectiveTransform(pothole_reshaped, H).reshape(
            -1, 2
        )  # mask in BEV

        # Draw pothole on BEV image
        bev_with_pothole = bev_img.copy()
        cv2.fillPoly(bev_with_pothole, [pothole_bev.astype(np.int32)], (255, 0, 100))
        cv2.polylines(
            bev_with_pothole, [pothole_bev.astype(np.int32)], True, (0, 255, 0), 2
        )

        # Step 5: Calculate area
        pothole_homo = np.column_stack(
            [undistorted_pothole, np.ones(len(undistorted_pothole))]
        )
        pothole_transformed = (H @ pothole_homo.T).T
        pothole_transformed = pothole_transformed[:, :2] / pothole_transformed[:, 2:]

        area_cm2 = self.polygon_area(pothole_transformed)

        return area_cm2, bev_img, H, pothole_bev

    def process_image_with_mask(self, image_path, mask_data, output_dir):
        """
        Process a single image with its pothole mask and calculate area.

        Args:
            image_path: Path to the input image
            mask_data: Dictionary containing mask information {conf, coordinates}
            output_dir: Directory to save results

        Returns: Enriched mask data with area and timestamp
        """
        # Load image
        img = cv2.imread(image_path)
        if img is None:
            raise ValueError(f"Could not load image: {image_path}")
        img_rgb = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)

        # Load pothole mask coordinates
        pothole_mask = np.asarray(mask_data["coordinates"])

        # Compute area
        area_cm2, bev_img, H, bev_mask = self.compute_pothole_area(
            img_rgb, pothole_mask
        )

        # Create output filename based on input image
        base_name = os.path.splitext(os.path.basename(image_path))[0]

        # Save BEV image for visualization (optional)
        bev_output_path = os.path.join(output_dir, f"{base_name}_bev.jpg")
        cv2.imwrite(bev_output_path, cv2.cvtColor(bev_img, cv2.COLOR_RGB2BGR))

        # Enrich mask data with area and timestamp
        enriched_data = {
            "detection_confidence": mask_data["conf"],
            "original_mask": mask_data["coordinates"],
            "bev_mask": bev_mask.tolist(),
            "homography_matrix": H.tolist(),
            "surface_area": area_cm2,
            "timestamp": datetime.now().isoformat(sep="T", timespec="milliseconds"),
        }

        return enriched_data

    def process_batch(self, input_dir, output_dir):
        """
        Process all images and their corresponding masks in a directory.
        Enriches the JSON mask files with area calculations and timestamps.
        Copies original images to the output directory.

        Args:
            input_dir: Directory containing images and the corresponding .json mask files
            output_dir: Directory to save enriched JSON files, BEV images, and original images

        Returns:
            Tuple of (processed_count, total_area)
        """
        os.makedirs(output_dir, exist_ok=True)

        processed_count = 0
        total_area = 0

        # Find all image files with corresponding .json masks
        for filename in os.listdir(input_dir):
            if filename.endswith((".jpg", ".png", ".jpeg")):
                image_path = os.path.join(input_dir, filename)
                base_name = os.path.splitext(filename)[0]
                mask_file = os.path.join(input_dir, base_name + "_mask.json")

                if os.path.exists(mask_file):
                    try:
                        # Load mask data
                        with open(mask_file, "r") as f:
                            masks = json.load(f)  # List of {conf, coordinates}

                        enriched_masks = []

                        # Process each mask in the image
                        for idx, mask_data in enumerate(masks):
                            try:
                                enriched_data = self.process_image_with_mask(
                                    image_path, mask_data, output_dir
                                )
                                enriched_masks.append(enriched_data)

                                area = enriched_data["area_cm2"]
                                total_area += area
                                processed_count += 1

                                print(
                                    f"✓ Processed {base_name} mask #{idx}: "
                                    f"{area:.2f} cm² (conf: {mask_data['conf']:.3f})"
                                )

                            except Exception as e:
                                print(
                                    f"✗ Error processing mask #{idx} from {filename}: {e}"
                                )
                                # Keep original data if processing fails
                                enriched_masks.append(mask_data)

                        # Save enriched JSON to output directory
                        output_json_path = os.path.join(output_dir, base_name + ".json")
                        with open(output_json_path, "w") as f:
                            json.dump(enriched_masks, f, indent=2)

                        # Copy original image to output directory
                        output_image_path = os.path.join(output_dir, filename)
                        shutil.copy2(image_path, output_image_path)

                        print(f"✓ Saved enriched data to {output_json_path}")
                        print(f"✓ Copied original image to {output_image_path}")

                    except Exception as e:
                        print(f"✗ Error processing {filename}: {e}")

        print(f"\n{'='*70}")
        print("Batch processing complete!")
        print(f"Total potholes processed: {processed_count}")
        print(f"Total area detected: {total_area:.2f} cm² ({total_area/10000:.4f} m²)")
        print(f"Results saved to: {output_dir}")
        print(f"{'='*70}")

        return processed_count, total_area

    def delete_processed(self, filenames: list, dir: str):
        """
        Delete processed files to save space.

        Args:
            filenames: List of file names to delete
            dir: Directory where files are located
        """
        for filename in filenames:
            file_path = os.path.join(dir, filename)
            if os.path.exists(file_path):
                os.remove(file_path)
