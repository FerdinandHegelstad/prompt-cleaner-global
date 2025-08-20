#!probability_sampler.py
import math
import random
import statistics
from pathlib import Path
from typing import List, Tuple, Dict, Any
import json


def analyze_prompt_lengths(file_path: str) -> Dict[str, float]:
    """Analyze the character lengths of prompts in the given file.

    Args:
        file_path: Path to the file containing prompts

    Returns:
        Dictionary with statistics about prompt lengths
    """
    path = Path(file_path)
    lines = path.read_text(encoding='utf-8').splitlines()
    items = [line for line in lines if line]
    lengths = [len(item) for item in items]

    return {
        'count': len(items),
        'mean': statistics.mean(lengths),
        'std': statistics.stdev(lengths),
        'min': min(lengths),
        'max': max(lengths)
    }


def normal_pdf(x: float, mean: float, std: float) -> float:
    """Calculate the probability density function for a normal distribution.

    Args:
        x: The value to evaluate
        mean: Mean of the distribution
        std: Standard deviation of the distribution

    Returns:
        Probability density at x
    """
    variance = std ** 2
    return (1 / math.sqrt(2 * math.pi * variance)) * math.exp(-((x - mean) ** 2) / (2 * variance))


def custom_curve_pdf(x: float, mean: float, target_max: float = 200.0) -> float:
    """Calculate a custom probability density function that peaks at mean and gradually decreases to target_max.

    This creates a curve that:
    - Peaks sharply at the mean
    - Gradually decreases towards the target_max
    - Creates a "circle shaped curve from top" effect

    Args:
        x: The value to evaluate
        mean: Peak of the distribution
        target_max: Where the curve should approach zero

    Returns:
        Probability density at x
    """
    if x < 0:
        return 0.0

    # For values at or below the mean, use a steep curve
    if x <= mean:
        # Steep drop for values much lower than mean
        distance_from_peak = mean - x
        steepness_factor = 2.0  # Controls how steep the drop is
        return math.exp(-distance_from_peak * steepness_factor)

    # For values above the mean, use a gradual curve towards target_max
    else:
        distance_from_peak = x - mean
        distance_to_target = target_max - mean

        if distance_to_target <= 0:
            return 0.0

        # Normalize the distance (0 to 1)
        normalized_distance = distance_from_peak / distance_to_target

        # Create a gradual curve using exponential decay
        # The 0.8 controls how gradual the decline is
        decay_factor = 0.8
        curve_value = math.exp(-normalized_distance * decay_factor)

        # Add a small linear component for extra smoothness
        linear_factor = 1.0 - normalized_distance
        combined_value = (curve_value * 0.7) + (linear_factor * 0.3)

        # Ensure it doesn't go negative
        return max(0.0, combined_value)


def calculate_sampling_weights(items: List[str], mean: float, std: float) -> List[float]:
    """Calculate sampling weights based on custom curve probability.

    Args:
        items: List of items to calculate weights for
        mean: Mean length for the distribution (peak point)
        std: Not used in custom curve, kept for compatibility

    Returns:
        List of weights corresponding to each item
    """
    weights = []
    target_max = 200.0  # Custom curve extends to 200 characters

    for item in items:
        length = len(item)
        # Calculate probability density for this item's length using custom curve
        weight = custom_curve_pdf(length, mean, target_max)
        weights.append(weight)

    return weights


def probabilistic_sample(items: List[str], n: int, mean: float, std: float) -> List[str]:
    """Sample items using probability weights based on normal distribution.

    Args:
        items: List of items to sample from
        n: Number of items to sample
        mean: Mean length for the normal distribution
        std: Standard deviation for the normal distribution

    Returns:
        List of sampled items
    """
    if len(items) <= n:
        return items.copy()

    # Calculate weights based on normal distribution
    weights = calculate_sampling_weights(items, mean, std)

    # Normalize weights to sum to 1
    total_weight = sum(weights)
    if total_weight == 0:
        # Fallback to uniform sampling if all weights are 0
        return random.sample(items, n)

    normalized_weights = [w / total_weight for w in weights]

    # Sample without replacement using the weights
    selected_items = []
    remaining_items = items.copy()
    remaining_weights = normalized_weights.copy()

    for _ in range(min(n, len(items))):
        if not remaining_items:
            break

        # Select item based on weights
        chosen_idx = random.choices(range(len(remaining_items)), weights=remaining_weights, k=1)[0]
        selected_items.append(remaining_items[chosen_idx])

        # Remove selected item and its weight
        remaining_items.pop(chosen_idx)
        remaining_weights.pop(chosen_idx)

        # Renormalize weights
        if remaining_weights:
            total_remaining = sum(remaining_weights)
            if total_remaining > 0:
                remaining_weights = [w / total_remaining for w in remaining_weights]

    return selected_items


def get_distribution_curve(mean: float, std: float, min_length: int = 5, max_length: int = 300, steps: int = 100) -> List[Tuple[float, float]]:
    """Generate points for plotting the custom distribution curve.

    Args:
        mean: Mean of the distribution (peak point)
        std: Not used in custom curve, kept for compatibility
        min_length: Minimum length for the curve
        max_length: Maximum length for the curve
        steps: Number of points to generate

    Returns:
        List of (x, y) tuples for plotting
    """
    curve_points = []
    target_max = 200.0

    for i in range(steps):
        x = min_length + (max_length - min_length) * i / (steps - 1)
        y = custom_curve_pdf(x, mean, target_max)
        curve_points.append((x, y))

    return curve_points


def save_length_statistics(stats: Dict[str, float], filename: str = "length_stats.json") -> None:
    """Save length statistics to a JSON file.

    Args:
        stats: Dictionary containing statistics
        filename: Name of the file to save to
    """
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(stats, f, indent=2)


def load_length_statistics(filename: str = "length_stats.json") -> Dict[str, float]:
    """Load length statistics from a JSON file.

    Args:
        filename: Name of the file to load from

    Returns:
        Dictionary containing statistics
    """
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
