import dataclasses
from collections.abc import Sequence, Callable
from dataclasses import dataclass
from functools import reduce
from typing import Optional, Any

from pandas import DataFrame

from source.bw_libs.base_class import BaseConfig, VerboseLevel, base_method, BaseClass
from source.bw_libs.db_manager import DBManager
from source.bw_libs.helper import Helper
from source.db_tables import Params, Layers
from source.types.logger_types import TermLoggerType
from source.types.pipeline_params_types import LayerParams, PipelineParams, PipelineParamsCombinations


@dataclass
class Config(BaseConfig):
    dbconn_username: str
    dbconn_dbname: str
    hash_param_key: str = 'Hash'
    stack_param_key: str = 'Stack'


class PipelineParamsManager(BaseClass):

    def __init__(self, config: Optional[dict] = None, default_verbose_level: Optional[VerboseLevel] = None):
        super().__init__(Config, config, default_verbose_level)

        self.__db_manager = None
        self.__initialize_dbm()

    @base_method
    def __initialize_dbm(self):
        self.__db_manager = DBManager(config={'conn_username': self._config.dbconn_username,
                                              'conn_dbname': self._config.dbconn_dbname,
                                              'logs_folder': self._config.logs_folder},
                                      # default_verbose_level=VerboseLevel.LOCAL,
                                      )

    @base_method
    def __clear_empty_params(self, params: dict) -> dict:
        copied_params = params.copy()
        for key, values in zip(params.keys(), params.values()):
            if values is None or (isinstance(values, Sequence) and len(values) == 0):
                del copied_params[key]
        return copied_params

    @base_method
    def __generate_cartesian_product(self, factors: dict[str | int, list[Any]]) -> list[dict[str | int, Any]]:
        total_combinations = reduce(lambda a, b: (a * len(b)), factors.values(), 1)
        products_list = []

        if self._dynamic_verbose_level != VerboseLevel.NONE:
            self._logger.debug(TermLoggerType.SHORT, f'total_combinations: {total_combinations}')

        for counter in range(total_combinations):
            quotient = counter
            product = {}
            for key, values in zip(factors.keys(), factors.values()):
                quotient, remainder = divmod(quotient, len(values))
                product[key] = values[remainder]

                if self._dynamic_verbose_level != VerboseLevel.NONE:
                    self._logger.debug(TermLoggerType.SHORT, f'counter: {counter}')
                    self._logger.debug(TermLoggerType.SHORT, f'key: {key} | values({len(values)}): {str(values)}')
                    self._logger.debug(TermLoggerType.SHORT, f'quotient: {quotient} | remainder: {remainder}')

            products_list.append(product)

            if self._dynamic_verbose_level != VerboseLevel.NONE:
                self._logger.debug(TermLoggerType.SHORT, f'product:\n{Helper.beautify_json(product)}')

        return products_list

    @base_method
    def __build_objects(self, plain_data: list[dict[str, Any]]) -> list[PipelineParams]:
        def build_stack(stack: dict[int, dict[str, Any]]) -> dict[int, LayerParams]:
            built_stack = {}
            for key, values in zip(stack.keys(), stack.values()):
                built_stack[key] = LayerParams(**values)
            return built_stack

        built_objects = []
        for data_dict in plain_data:
            data_dict[self._config.hash_param_key] = Helper.generate_dict_hash(data_dict)
            data_dict[self._config.stack_param_key] = build_stack(
                data_dict[self._config.stack_param_key])
            built_objects.append(PipelineParams(**data_dict))

        return built_objects

    @base_method
    def unfold_combinations(self, pipeline_combinations: PipelineParamsCombinations) -> list[PipelineParams]:
        plain_pipeline_combinations = dataclasses.asdict(pipeline_combinations)
        plain_mutable_pipeline = plain_pipeline_combinations.copy()
        for key, values in zip(plain_pipeline_combinations.keys(), plain_pipeline_combinations.values()):
            match key:
                case self._config.stack_param_key:
                    unfolded_stacks = []
                    for stack in values:
                        unfolded_layers_stack = {}
                        for layer_index in range(len(stack)):
                            layer_combinations = stack[layer_index]
                            layer_combinations = self.__clear_empty_params(layer_combinations)
                            unfolded_layer = self.__generate_cartesian_product(layer_combinations)
                            unfolded_layers_stack[layer_index] = unfolded_layer
                        unfolded_stacks.extend(self.__generate_cartesian_product(unfolded_layers_stack))
                    plain_mutable_pipeline[key] = unfolded_stacks
                    break
        unfolded_pipeline = self.__generate_cartesian_product(plain_mutable_pipeline)
        return self.__build_objects(unfolded_pipeline)

    @base_method
    def store_in_db(self, pipeline_params: Sequence[PipelineParams]):
        code_version = Helper.get_last_git_tag()

        def stringify_callable(obj: Callable) -> str:
            if obj is None:
                return str(obj)
            else:
                return Helper.get_fully_qualified_name(obj)

        def store_params():
            def build_params_records() -> list[Params]:
                params_records = []
                for params in pipeline_params:
                    params_records.append(Params(
                        Hash=params.Hash,
                        CodeVersion=code_version,
                        ColToPredict=params.ColumnToPredict,
                        WindowWidth=params.WindowWidth,
                        SetTrainingFlag=params.SetTrainingFlag,
                        UseResidualWrapper=params.UseResidualWrapper,
                        PrependBatchNormLayer=params.PrependBatchNormLayer,
                        FitMaxEpochs=params.FitMaxEpochs,
                        FitPatience=params.FitPatience,
                        CompileLossFn=stringify_callable(params.CompileLossFunction),
                        CompileOptimizer=stringify_callable(params.CompileOptimizer),
                        DatasetPath=str(params.DatasetPath),
                        DatasetTimeFilter=str(params.DatasetTimeFilter),
                        DatasetShuffle=params.DatasetShuffle,
                        DatasetBatchSize=params.DatasetBatchSize,
                    ))
                return params_records

            params_records = build_params_records()
            self.__db_manager.insert(params_records)

        store_params()
        new_hashes = list(map(lambda params: (params.Hash), pipeline_params))

        def get_new_signatures() -> DataFrame:
            new_signatures = self.__db_manager.get_columns(columns=[Params.ID, Params.Hash, Params.CodeVersion],
                                                           filter_criterion=[Params.Hash.in_(new_hashes)])
            return new_signatures

        new_signatures = get_new_signatures()

        def store_layers():
            def build_layers_records() -> list[Layers]:
                layers_records = []
                for params in pipeline_params:
                    for layer_index, layer_params in zip(params.Stack.keys(), params.Stack.values()):
                        params_signature = new_signatures.loc[(new_signatures['Hash'] == params.Hash)
                                                              & (new_signatures['CodeVersion'] == code_version)]
                        params_id = params_signature.ID.iloc[0]
                        layers_records.append(Layers(
                            ParamsID=int(params_id),
                            LayerIndex=layer_index,
                            Units=layer_params.Units,
                            KernelInitializer=stringify_callable(layer_params.KernelInitializer),
                            KernelRegularizer=stringify_callable(layer_params.KernelRegularizer),
                            Activation=stringify_callable(layer_params.Activation),
                        ))
                return layers_records

            layers_records = build_layers_records()
            self.__db_manager.insert(layers_records)

        store_layers()

    @base_method
    def destroy(self):
        super().destroy()
        self.__db_manager.destroy()
