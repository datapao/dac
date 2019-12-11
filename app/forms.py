from flask_wtf import FlaskForm
from wtforms import StringField, SelectField, SubmitField, FloatField
from wtforms.validators import DataRequired, Optional


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
    interactive_dbu_price = FloatField('Cost / Interactive DBU', validators=[Optional()])
    job_dbu_price = FloatField('Cost / Job DBU', validators=[Optional()])
    price_submit = SubmitField('Set', render_kw={"class": "button"})

    def validate(self):
        if not super().validate():
            return False
        if not self.interactive_dbu_price.data and not self.job_dbu_price.data:
            msg = 'At least one of the price must be set'
            self.interactive_dbu_price.errors.append(msg)
            self.job_dbu_price.errors.append(msg)
            return False
        return True
