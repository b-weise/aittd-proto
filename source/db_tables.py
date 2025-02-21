from datetime import datetime

from sqlalchemy import String, DateTime, Integer, Boolean, ForeignKey, UniqueConstraint
from sqlalchemy.orm import mapped_column, relationship

from source.bw_libs.db_manager import DBManager


class EnumStatus(DBManager.Base):
    __tablename__ = 'EnumStatus'
    ID = mapped_column(Integer(), primary_key=True)
    Description = mapped_column(String(128))
    CreatedOn = mapped_column(DateTime(), default=datetime.now)
    UpdatedOn = mapped_column(DateTime(), default=datetime.now, onupdate=datetime.now)
    states_rel = relationship('States', back_populates='enumStatus_rel')


class Params(DBManager.Base):
    __tablename__ = 'Params'
    ID = mapped_column(Integer(), primary_key=True)
    Hash = mapped_column(String(128), nullable=False)
    CodeVersion = mapped_column(String(64), nullable=False)
    ColToPredict = mapped_column(String(128))
    WindowWidth = mapped_column(Integer())
    SetTrainingFlag = mapped_column(Boolean())
    UseResidualWrapper = mapped_column(Boolean())
    PrependBatchNormLayer = mapped_column(Boolean())
    FitMaxEpochs = mapped_column(Integer())
    FitPatience = mapped_column(Integer())
    CompileLossFn = mapped_column(String(128))
    CompileOptimizer = mapped_column(String(128))
    DatasetPath = mapped_column(String(256))
    DatasetTimeFilter = mapped_column(String(128))
    DatasetShuffle = mapped_column(Boolean())
    DatasetBatchSize = mapped_column(Integer())
    CreatedOn = mapped_column(DateTime(), default=datetime.now)
    UpdatedOn = mapped_column(DateTime(), default=datetime.now, onupdate=datetime.now)
    training_rel = relationship('Training', back_populates='params_rel')
    states_rel = relationship('States', back_populates='params_rel')
    layers_rel = relationship('Layers', back_populates='params_rel')
    __table_args__ = (UniqueConstraint('Hash', 'CodeVersion'),)


class Layers(DBManager.Base):
    __tablename__ = 'Layers'
    ParamsID = mapped_column(ForeignKey(Params.ID), primary_key=True)
    LayerIndex = mapped_column(Integer(), primary_key=True)
    Units = mapped_column(Integer())
    KernelInitializer = mapped_column(String(128))
    KernelRegularizer = mapped_column(String(128))
    Activation = mapped_column(String(128))
    CreatedOn = mapped_column(DateTime(), default=datetime.now)
    UpdatedOn = mapped_column(DateTime(), default=datetime.now, onupdate=datetime.now)
    params_rel = relationship('Params', back_populates='layers_rel')


class Training(DBManager.Base):
    __tablename__ = 'Training'
    ParamsID = mapped_column(ForeignKey(Params.ID), primary_key=True)
    DurationString = mapped_column(String(64))
    DurationInSec = mapped_column(Integer())
    ModelPath = mapped_column(String(256))
    CreatedOn = mapped_column(DateTime(), default=datetime.now)
    UpdatedOn = mapped_column(DateTime(), default=datetime.now, onupdate=datetime.now)
    params_rel = relationship('Params', back_populates='training_rel')


class States(DBManager.Base):
    __tablename__ = 'States'
    ParamsID = mapped_column(ForeignKey(Params.ID), primary_key=True)
    Status = mapped_column(ForeignKey(EnumStatus.ID))
    SetBy = mapped_column(String(64))  # hostname
    CreatedOn = mapped_column(DateTime(), default=datetime.now)
    UpdatedOn = mapped_column(DateTime(), default=datetime.now, onupdate=datetime.now)
    params_rel = relationship('Params', back_populates='states_rel')
    enumStatus_rel = relationship('EnumStatus', back_populates='states_rel')
