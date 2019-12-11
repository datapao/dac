from flask_wtf import FlaskForm
from wtforms import StringField, SelectField, SubmitField, FloatField
from wtforms.validators import DataRequired


class WorkspaceForm(FlaskForm):
    urlfield = StringField('url',
                           validators=[DataRequired()],
                           render_kw={
                               "placeholder": "myregion.azuredatabricks.net"})
    idfield = StringField('id',
                          validators=[DataRequired()],
                          render_kw={
                              "placeholder": "0123456789876543210"})
    typefield = SelectField('type',
                            choices=[('AZURE', 'AZURE'), ('AWS', 'AWS')])
    namefield = StringField('name',
                            validators=[DataRequired()],
                            render_kw={
                                "placeholder": "my-azure-workspace"})
    tokenfield = StringField('token',
                             validators=[DataRequired()],
                             render_kw={"placeholder": ("dapi00123456789abcdef"
                                                        "edcba9876543210")})

    workspace_submit = SubmitField('Scrape', render_kw={"class": "button"})


class PriceForm(FlaskForm):
    price = FloatField('Cost / DBU', validators=[DataRequired()])
    price_submit = SubmitField('Set', render_kw={"class": "button"})
