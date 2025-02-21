from collections.abc import Sequence, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class LayerParams:
    Units: int
    KernelInitializer: Optional[Callable] = None
    KernelRegularizer: Optional[Callable] = None
    Activation: Optional[Callable] = None


@dataclass
class PipelineParams:
    Hash: str
    ColumnToPredict: str
    WindowWidth: int
    SetTrainingFlag: bool
    UseResidualWrapper: bool
    PrependBatchNormLayer: bool
    FitMaxEpochs: int
    FitPatience: int
    CompileLossFunction: Callable
    CompileOptimizer: Callable
    Stack: dict[int, LayerParams]
    DatasetPath: Path
    DatasetTimeFilter: Sequence[str]
    DatasetShuffle: bool
    DatasetBatchSize: int


@dataclass
class LayerParamsCombinations:
    Units: Sequence[int]
    KernelInitializer: Optional[Sequence[Callable]] = None
    KernelRegularizer: Optional[Sequence[Callable]] = None
    Activation: Optional[Sequence[Callable]] = None


@dataclass
class PipelineParamsCombinations:
    ColumnToPredict: Sequence[str]
    WindowWidth: Sequence[int]
    SetTrainingFlag: Sequence[bool]
    UseResidualWrapper: Sequence[bool]
    PrependBatchNormLayer: Sequence[bool]
    FitMaxEpochs: Sequence[int]
    FitPatience: Sequence[int]
    CompileLossFunction: Sequence[Callable]
    CompileOptimizer: Sequence[Callable]
    Stack: Sequence[dict[int, LayerParamsCombinations]]
    DatasetPath: Sequence[Path]
    DatasetTimeFilter: Sequence[Sequence[str]]
    DatasetShuffle: Sequence[bool]
    DatasetBatchSize: Sequence[int]
