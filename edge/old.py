import argparse
from segmentation import (
    PotholeSegmentationYOLO,
    PotholeSegmentationRFDETR,
)
from surface_area import PotholeAreaEstimator
from config_loader import load_config


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Pothole Detection and Area Estimation Pipeline"
    )
    parser.add_argument(
        "--config",
        type=str,
        default="config.yaml",
        help="Path to configuration file (default: config.yaml)",
    )
    args = parser.parse_args()

    # Load configuration
    config = load_config(args.config)
    config.print_config_summary()

    print("=" * 70)
    print(" POTHOLE DETECTION AND AREA ESTIMATION PIPELINE")
    print("=" * 70)
    print()

    # ========================================================================
    # STEP 1: VIDEO SEGMENTATION
    # ========================================================================
    print("STEP 1: Video Segmentation")
    print("-" * 70)

    # Select model based on configuration
    model_type = config.get_model_type()
    output_dirs = config.get_output_dirs()

    if model_type == "yolo":
        segmenter = PotholeSegmentationYOLO(
            model_path=config.get_model_path(),
            trapezoid_coords=config.get_trapezoid_coords(),
            confidence_threshold=config.get_confidence_threshold(),
            frame_interval=config.get_frame_interval(),
        )
    elif model_type == "rfdetr":
        segmenter = PotholeSegmentationRFDETR(
            model_path=config.get_model_path(),
            trapezoid_coords=config.get_trapezoid_coords(),
            confidence_threshold=config.get_confidence_threshold(),
            frame_interval=config.get_frame_interval(),
        )
    else:
        raise ValueError(f"Unsupported model type: {model_type}")

    try:
        num_frames_detected = segmenter.process_video(
            video_path=config.get_video_path(),
            output_dir=output_dirs["segmentation"],
        )
    except ValueError as e:  # no video path provided or could not open video
        print(e)
        return

    if num_frames_detected == 0:
        print("\n⚠ No potholes detected in video. Exiting.")
        return

    print()

    # ========================================================================
    # STEP 2: AREA ESTIMATION
    # ========================================================================
    print("STEP 2: Area Estimation")
    print("-" * 70)

    estimator = PotholeAreaEstimator(
        trapezoid_coords=config.get_trapezoid_coords(),
        rectangle_coords=config.get_rectangle_coords(),
        calibration_path=config.get_calibration_path(),
    )

    # Process all segmented frames
    num_processed, total_area = estimator.process_batch(
        input_dir=output_dirs["segmentation"],
        output_dir=output_dirs["area_estimation"],
    )

    print()

    # ========================================================================
    # SUMMARY
    # ========================================================================
    print("=" * 70)
    print(" PIPELINE COMPLETE")
    print("=" * 70)
    print(f"Frames with potholes detected: {num_frames_detected}")
    print(f"Images processed for area estimation: {num_processed}")
    print(f"Total pothole area: {total_area:.2f} cm² ({total_area/10000:.4f} m²)")
    print()
    print(f"Segmentation results: {output_dirs['segmentation']}")
    print(f"Area estimation results: {output_dirs['area_estimation']}")
    print("=" * 70)


if __name__ == "__main__":
    main()
