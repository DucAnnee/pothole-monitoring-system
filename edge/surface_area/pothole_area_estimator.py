import cv2
import json
import numpy as np
import os
from datetime import datetime


class PotholeAreaEstimator:
    def __init__(
        self,
        trapezoid_coords,
        rectangle_coords,
        reference_resolution=None,
        calibration_path="outputs/camera_calibration.json",
    ):
        """
        Initialize the pothole area estimator.

        Args:
            trapezoid_coords: np.array of shape (4, 2) - detection region coordinates
            rectangle_coords: np.array of shape (4, 2) - BEV rectangle template (in cm)
            reference_resolution: (width, height) tuple of the resolution where trapezoid was defined.
                                  If None, trapezoid_coords are treated as normalized [0,1] coordinates.
            calibration_path: Path to camera calibration JSON
        """
        self.rectangle_coords = rectangle_coords
        self.calibration_path = calibration_path

        # Normalize trapezoid coordinates to [0, 1] range
        if reference_resolution is not None:
            # Trapezoid coords are in pixel space, normalize them
            ref_w, ref_h = reference_resolution
            self.normalized_trapezoid = trapezoid_coords / np.array([ref_w, ref_h])
        else:
            # Already normalized
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
        pothole_bev = cv2.perspectiveTransform(pothole_reshaped, H).reshape(-1, 2)

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

        return area_cm2, bev_img, H

    def process_image_with_mask(self, image_path, mask_path, output_dir):
        """
        Process a single image with its pothole mask and save results.

        Args:
            image_path: Path to the input image
            mask_path: Path to the numpy file containing pothole mask
            output_dir: Directory to save results

        Returns: area_cm2
        """
        # Load image
        img = cv2.imread(image_path)
        if img is None:
            raise ValueError(f"Could not load image: {image_path}")
        img_rgb = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)

        # Load pothole mask
        pothole_mask = np.load(mask_path, allow_pickle=True)

        # Compute area
        area_cm2, bev_img, H = self.compute_pothole_area(img_rgb, pothole_mask)

        # Create output filename based on mask filename (preserves pothole index)
        mask_base_name = os.path.splitext(os.path.basename(mask_path))[0]
        # Remove "_mask_0" suffix and add "_pothole_0" for clarity
        # e.g., "frame_000030_timestamp_mask_0" -> "frame_000030_timestamp_pothole_0"
        if "_mask_" in mask_base_name:
            output_base = mask_base_name.replace("_mask_", "_pothole_")
        else:
            output_base = mask_base_name

        # Save BEV image
        img_path = os.path.join(output_dir, f"{output_base}_bev.jpg")
        cv2.imwrite(img_path, cv2.cvtColor(bev_img, cv2.COLOR_RGB2BGR))

        # Save area information
        txt_path = os.path.join(output_dir, f"{output_base}_area.txt")
        with open(txt_path, "w") as f:
            f.write(f"Pothole Surface Area: {area_cm2:.2f} cm²\n")
            f.write(f"Pothole Surface Area: {area_cm2/10000:.4f} m²\n")
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            f.write(f"Processing Timestamp: {timestamp}\n")

        print(f"✓ Processed {output_base}: {area_cm2:.2f} cm²")

        return area_cm2

    def process_batch(self, input_dir, output_dir):
        """
        Process all images and their corresponding masks in a directory.

        Args:
            input_dir: Directory containing images and .npy mask files
            output_dir: Directory to save results
        """
        os.makedirs(output_dir, exist_ok=True)

        # Find all image files with corresponding .npy masks
        processed_count = 0
        total_area = 0

        for filename in os.listdir(input_dir):
            if filename.endswith((".jpg", ".png", ".jpeg")):
                image_path = os.path.join(input_dir, filename)
                base_name = os.path.splitext(filename)[0]

                # Find all masks for this image (e.g., base_name_mask_0.npy, base_name_mask_1.npy)
                import glob

                mask_pattern = os.path.join(input_dir, f"{base_name}_mask_*.npy")
                mask_files = glob.glob(mask_pattern)

                if mask_files:
                    # Process each mask for this image
                    for mask_path in sorted(mask_files):
                        try:
                            area = self.process_image_with_mask(
                                image_path, mask_path, output_dir
                            )
                            processed_count += 1
                            total_area += area
                        except Exception as e:
                            print(
                                f"✗ Error processing {os.path.basename(mask_path)}: {e}"
                            )
                else:
                    print(f"⚠ Skipping {filename}: No mask files found")

        print(f"\n{'='*50}")
        print(f"Batch processing complete!")
        print(f"Total images processed: {processed_count}")
        print(f"Total area detected: {total_area:.2f} cm² ({total_area/10000:.4f} m²)")
        print(f"Results saved to: {output_dir}")
        print(f"{'='*50}")

        return processed_count, total_area
