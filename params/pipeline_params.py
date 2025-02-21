import keras

from source.bw_libs.helper import Helper
from source.types.pipeline_params_types import (LayerParamsCombinations,
                                                PipelineParamsCombinations)


class InputPipelineParams:

    @staticmethod
    def get() -> PipelineParamsCombinations:
        output_layer = 0
        return PipelineParamsCombinations(
            ColumnToPredict=['Oracle'],
            WindowWidth=[300],
            SetTrainingFlag=[True],
            UseResidualWrapper=[False],
            PrependBatchNormLayer=[True],
            FitMaxEpochs=[2],
            FitPatience=[50],
            CompileLossFunction=[keras.losses.MeanAbsoluteError],
            CompileOptimizer=[keras.optimizers.RMSprop],
            Stack=[
                {
                    output_layer: LayerParamsCombinations(
                        Units=[0],  # 0 = len(col_to_predict); -1 = num_features
                        Activation=[keras.activations.relu, keras.activations.sigmoid],
                    ),
                    1: LayerParamsCombinations(
                        Units=[8, 16],
                        KernelInitializer=[keras.initializers.Zeros],
                    ),
                },
                {
                    output_layer: LayerParamsCombinations(
                        Units=[32, 64],  # 0 = len(col_to_predict); -1 = num_features
                        KernelRegularizer=[keras.regularizers.L1L2],
                    ),
                }
            ],
            DatasetPath=Helper.build_paths(['dataset.csv']),
            DatasetTimeFilter=[('2024-01-01 00:00', '2024-02-01 00:00')],
            # [from, to); None: means no limit in that direction
            DatasetShuffle=[True, False],
            DatasetBatchSize=[8, 16, 32],
        )
