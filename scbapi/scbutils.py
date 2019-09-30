from typing import List, Dict
import mapclassify
import pandas

Colorscale = List[List[str]]


class MappingTools(object):
    CLASSIFIERS: Dict[str, str] = {
        "FISHER_JENKS":     "Fisher-Jenks",
        "QUANTILES":        "Quantiles",
        "MAXIMUM_BREAKS":   "Maximum breaks",
        "NATURAL_BREAKS":   "Natural breaks"
    }

    @staticmethod
    def get_colorscale(df: pandas.DataFrame, column: str, colors: List[str], classifier: str = None) -> 'Colorscale':
        """
        Returns color scale using the selected classifier
        """
        # Check if column exists
        if column not in df:
            return []

        # Prepare normalization for color scale values
        col_to_norm: List = df[column]

        # If the number of values is lower than the number of bins return empty
        if len(col_to_norm) < len(colors) - 1:
            return []

        norm_vals: List = ((col_to_norm - min(col_to_norm)) / (max(col_to_norm) - min(col_to_norm))).astype(float)

        if classifier is None:
            bins = mapclassify.Fisher_Jenks(norm_vals, k=len(colors) - 1).bins.tolist()
        elif MappingTools.CLASSIFIERS[classifier] == MappingTools.CLASSIFIERS["FISHER_JENKS"]:
            bins = mapclassify.Fisher_Jenks(norm_vals, k=len(colors) - 1).bins.tolist()
        elif MappingTools.CLASSIFIERS[classifier] == MappingTools.CLASSIFIERS["QUANTILES"]:
            bins = mapclassify.Quantiles(norm_vals, k=len(colors) - 1).bins.tolist()
        elif MappingTools.CLASSIFIERS[classifier] == MappingTools.CLASSIFIERS["MAXIMUM_BREAKS"]:
            bins = mapclassify.Maximum_Breaks(norm_vals, k=len(colors) - 1).bins.tolist()
        elif MappingTools.CLASSIFIERS[classifier] == MappingTools.CLASSIFIERS["NATURAL_BREAKS"]:
            bins = mapclassify.Natural_Breaks(norm_vals, k=len(colors) - 1).bins.tolist()
        else:
            return []

        bins.insert(0, 0)

        colorscale: 'Colorscale' = [list(a) for a in zip(bins, colors)]
        return colorscale
