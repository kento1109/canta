"""
構造化レポートから「がん」を疑う所見を取り出し、解剖区域別に出力する
"""
import os
from typing import Literal

import owleyes.filters
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
import tensorflow as tf

import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

from absl import app
from absl import flags
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import or_
import logzero

from cicada.schema import Attribute, Surface, SurfacesAnatomicalConceptView, SurfaceClinicalConceptView, SurfaceChangeConceptView
import owleyes

logzero.loglevel(10)
logzero.logfile('result.log', mode="w")

flags.DEFINE_string("table_name", None, None)
flags.DEFINE_string("output_file_path", None, None)
flags.DEFINE_string("output_type", "surface_name", None)
flags.DEFINE_integer("debug_order_no", None, None)

FLAGS = flags.FLAGS

th_certainty_score = 1

client = owleyes.ClientV4()

engine = create_engine(
    "postgresql://postgres:password@localhost:15432/postgres")

malignant_id = 'A000001'
indeterminate_id = 'A000002'
undefined_id = 'A000017'

malignant_or_indeterminate_surfaces = SurfaceClinicalConceptView.query.filter(
    SurfaceClinicalConceptView.malignancy_id.in_(
        [malignant_id, indeterminate_id])).all()
malignant_or_indeterminate_surface_ids = set(
    [surface.surface_id for surface in malignant_or_indeterminate_surfaces])

tumor_possible_surfaces = SurfaceClinicalConceptView.query.filter(
    or_(SurfaceClinicalConceptView.disease_id == undefined_id,
        SurfaceClinicalConceptView.disease_id == None,
        SurfaceClinicalConceptView.malignancy_id != None)).all()

tumor_possible_surface_ids = set(
    [surface.surface_id for surface in tumor_possible_surfaces])

cystic_lesion_id = 'C000483'
cystic_lesion_surfaces = SurfaceClinicalConceptView.query.filter(
    SurfaceClinicalConceptView.concept_id == cystic_lesion_id).all()
cystic_lesion_surface_ids = set(
    [surface.surface_id for surface in cystic_lesion_surfaces])

pancreas_id = 'A000044'
pancreas_surfaces = SurfacesAnatomicalConceptView.query.filter(
    SurfacesAnatomicalConceptView.organ_id == pancreas_id).all()
pancreas_surface_ids = set(
    [surface.surface_id for surface in pancreas_surfaces])

worsening_id = 'A000073'
new_id = 'A000074'
changed_id = 'A000078'

worsening_surfaces = SurfaceChangeConceptView.query.filter(
    SurfaceChangeConceptView.progress_id.in_(
        [worsening_id, new_id, changed_id])).all()
worsening_surface_ids = set(
    [surface.surface_id for surface in worsening_surfaces])

filter_callbacks = [
    {
        'function': owleyes.filters.is_specified_clinical_object,
        'surface_ids': malignant_or_indeterminate_surface_ids,
        'certainty_score': th_certainty_score,
        'priority': 0,
    },
    {
        'function': owleyes.filters.has_specified_causations,
        'surface_ids': tumor_possible_surface_ids,
        'certainty_score': th_certainty_score,
        'priority': 1,
    },
    {
        'function': owleyes.filters.should_obj_followed,
        'priority': 2,
    },
    {
        'function': owleyes.filters.is_obs_worsening,
        'worsening_surface_ids': worsening_surface_ids,
        'priority': 3,
    },
]

surfaces = Surface.query.all()
surface_dict = {
    surface.surface_id: surface.surface_name
    for surface in surfaces
}

surface_clinical_concept = SurfaceClinicalConceptView.query.all()
surface_clinical_concept_dict = {
    surface_concept.surface_id: surface_concept
    for surface_concept in surface_clinical_concept
}

# organ_body_parts = cicada.schema.Attribute.query.filter(cicada.schema.Attribute.attribute_category == 'organ').all()
organ_body_parts = Attribute.query.filter(
    or_(Attribute.attribute_category == 'organ',
        Attribute.attribute_category == 'body_part')).all()
organ_body_part_dict = {
    organ_body_part.attribute_id: organ_body_part.attribute_name
    for organ_body_part in organ_body_parts
}
organ_body_part_dict['A000025'] = '骨'
organ_body_part_dict['A999999'] = 'その他'

organ_body_part_cols = [
    '甲状腺', '肺', '胸膜', '心臓', '乳房', '縦隔', '食道', '胃', '肝臓', '胆嚢', '胆道', '膵臓',
    '脾臓', '腎臓', '副腎', '小腸', '大腸', '腹膜', '子宮', '外陰', '膣', '卵巣', '前立腺', '精巣',
    '陰茎', '膀胱', '尿管', '尿道', '頸部', '胸部', '腹部', '骨盤部', '骨', 'その他'
]


def convert_to_dict(grouped_report,
                    output_type: Literal['surface_id', 'surface_name',
                                         'concept_id', 'malignancy_id']):
    raw_dict = {}
    raw_dict['order_no'] = grouped_report.order_no
    logzero.logger.debug(f"order no : {raw_dict['order_no']}")
    try:
        for organ_body_part_id in organ_body_part_dict:
            surface_ids = grouped_report.anatomical_group.get(
                organ_body_part_id, None)
            concepts = [
                surface_clinical_concept_dict[surface_id]
                for surface_id in surface_ids
            ] if surface_ids else None
            # if organ_body_part_id == pancreas_id:
            #     concepts = [concept if concept.concept_id != cystic_lesion_id else pancreas_cystic_lesion_concept for concept in concepts]
            if output_type == 'surface_name':
                raw_dict[organ_body_part_dict[organ_body_part_id]] = ':'.join(
                    [surface_dict[surface_id]
                     for surface_id in surface_ids]) if surface_ids else None
            elif output_type == 'concept_id':
                raw_dict[organ_body_part_dict[organ_body_part_id]] = ':'.join(
                    [concept.concept_id
                     for concept in concepts]) if concepts else None
            elif output_type == 'malignancy_id':
                raw_dict[organ_body_part_dict[organ_body_part_id]] = ':'.join(
                    [concept.malignancy_id
                     for concept in concepts]) if concepts else None
            else:
                raw_dict[organ_body_part_dict[organ_body_part_id]] = ':'.join(
                    surface_ids) if surface_ids else None
            if raw_dict[organ_body_part_dict[organ_body_part_id]]:
                logzero.logger.debug(
                    f'{organ_body_part_dict[organ_body_part_id]}:{raw_dict[organ_body_part_dict[organ_body_part_id]]}'
                )
        return raw_dict
    except Exception as e:
        print(raw_dict['order_no'], e)


def main(unused_argv):

    request = owleyes.schema.Request(key='malignancy_id',
                                     certainty_score=th_certainty_score,
                                     table_name=FLAGS.table_name)
    conditional = 'contains' if request.values else 'is_not_null'

    client.query(request, conditional).filter(
        filter_callbacks, debug_order_no=FLAGS.debug_order_no).group_by(
            'organ_lymph_node_bone_id')
    raws = [
        convert_to_dict(grouped_report, FLAGS.output_type)
        for grouped_report in client.grouped_reports
    ]
    if not FLAGS.output_file_path or FLAGS.debug_order_no:
        return
    result_df = pd.DataFrame(raws)
    # result_df[['order_no'] + organ_body_part_cols].to_excel(
    #     FLAGS.output_file_path, index=False)
    report_df = pd.read_sql(
        f'SELECT order_no, shoken, shindan FROM {FLAGS.table_name};', engine)
    # report_df['order_no'] = report_df['order_no'].astype(int)
    result_df['order_no'] = result_df['order_no'].astype(int)
    df = pd.merge(report_df, result_df, how='left')
    df[['order_no', 'shoken', 'shindan'] + organ_body_part_cols].to_excel(
        FLAGS.output_file_path, index=False)


if __name__ == "__main__":
    app.run(main)
