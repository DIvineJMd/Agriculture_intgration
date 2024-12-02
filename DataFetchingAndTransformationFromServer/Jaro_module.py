def jaro_similarity(s1, s2):
    """
    Calculate the Jaro similarity between two strings.

    Parameters:
        s1 (str): First string.
        s2 (str): Second string.

    Returns:
        float: Jaro similarity score.
    """
    if s1 == s2:
        return 1.0

    len_s1 = len(s1)
    len_s2 = len(s2)

    match_distance = max(len_s1, len_s2) // 2 - 1

    s1_matches = [False] * len_s1
    s2_matches = [False] * len_s2

    # Count matches
    matches = 0
    for i in range(len_s1):
        start = max(0, i - match_distance)
        end = min(len_s2, i + match_distance + 1)
        for j in range(start, end):
            if not s2_matches[j] and s1[i] == s2[j]:
                s1_matches[i] = True
                s2_matches[j] = True
                matches += 1
                break

    if matches == 0:
        return 0.0

    # Count transpositions
    s1_match_index = []
    s2_match_index = []

    for i in range(len_s1):
        if s1_matches[i]:
            s1_match_index.append(s1[i])

    for j in range(len_s2):
        if s2_matches[j]:
            s2_match_index.append(s2[j])

    transpositions = sum(1 for i, j in zip(s1_match_index, s2_match_index) if i != j) // 2

    # Jaro similarity formula
    jaro = (matches / len_s1 + matches / len_s2 + (matches - transpositions) / matches) / 3
    return jaro


def match_crop_names(input_name, crop_list, threshold=0.85):
    """
    Find crop names in the list that match the input name based on Jaro similarity.

    Parameters:
        input_name (str): Crop name to match.
        crop_list (list): List of crop names.
        threshold (float): Minimum similarity score to consider a match.

    Returns:
        list: List of matching crop names with their scores.
    """
    matches = []
    for crop in crop_list:
        similarity = jaro_similarity(input_name.lower(), crop.lower())
        if similarity >= threshold:
            matches.append((crop, similarity))
    return sorted(matches, key=lambda x: x[1], reverse=True)


# Example usage
crop_names = ["Corn", "Wheat", "Rice", "Barley", "Sorghum", "Soybean", "Maize"]
input_crop = input("Enter crop name to match: ")

results = match_crop_names(input_crop, crop_names)

if results:
    print("\nMatches found:")
    for crop, score in results:
        print(f"  {crop} (Similarity: {score:.2f})")
else:
    print("\nNo close matches found.")
