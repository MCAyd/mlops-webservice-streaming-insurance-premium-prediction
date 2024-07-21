from flask_wtf import FlaskForm
from wtforms import (StringField, IntegerField, SubmitField, SelectField, DateField)
from wtforms.validators import InputRequired, NumberRange, ValidationError
from datetime import datetime

class newCustomerForm(FlaskForm):

    # Maximum dates
    MAX_DATE_BIRTH = datetime(2000, 12, 31).date()
    MAX_DATE_LICENCE = datetime(2018, 12, 31).date()

    def validate_date_birth(form, field):
        if field.data > newCustomerForm.MAX_DATE_BIRTH:
            raise ValidationError(f"Date of Birth cannot be later than {newCustomerForm.MAX_DATE_BIRTH.strftime('%d-%m-%Y')}")

    def validate_date_licence(form, field):
        if field.data > newCustomerForm.MAX_DATE_LICENCE:
            raise ValidationError(f"Date of Licence cannot be later than {newCustomerForm.MAX_DATE_LICENCE.strftime('%d-%m-%Y')}")

    date_birth = DateField('Date of Birth', format='%Y-%m-%d', validators=[InputRequired()])
    date_licence = DateField('Date of Licence', format='%Y-%m-%d', validators=[InputRequired()])
    type_risk = SelectField(
    'Vehicle Type',
    choices=[(1, 'Motorbikes'), (2, 'Vans'), (3, 'Passenger Cars'), (4, 'Agricultural Vehicles')],
    validators=[InputRequired()]
    )
    second_driver = SelectField(
    'Second Driver',
    choices=[(0, 'No'), (1, 'Yes')],
    validators=[InputRequired()],
    coerce=int
    )
    area = SelectField(
    'Area',
    choices=[(0, 'Rural'), (1, 'Urban')],
    validators=[InputRequired()],
    coerce=int
    )
    power = IntegerField('Power', validators=[InputRequired(), NumberRange(min=0)])
    cylinder_capacity = IntegerField('Cylinder Capacity', validators=[InputRequired(), NumberRange(min=0)])
    value_vehicle = IntegerField('Value of Vehicle', validators=[InputRequired(), NumberRange(min=0)])
    type_fuel = SelectField(
    'Type of Fuel',
    choices=[('P', 'Petrol'), ('D', 'Diesel')],
    validators=[InputRequired()]
    )
    year_matriculation = IntegerField('Year of Matriculation', validators=[InputRequired(), NumberRange(min=1886, max=2018)])
    submit = SubmitField('Submit')

class existingCustomerForm(FlaskForm):
    
    type_risk = SelectField(
    'Vehicle Type',
    choices=[(1, 'Motorbikes'), (2, 'Vans'), (3, 'Passenger Cars'), (4, 'Agricultural Vehicles')],
    validators=[InputRequired()]
    )
    second_driver = SelectField(
    'Second Driver',
    choices=[(0, 'No'), (1, 'Yes')],
    validators=[InputRequired()],
    coerce=int
    )
    area = SelectField(
    'Area',
    choices=[(0, 'Rural'), (1, 'Urban')],
    validators=[InputRequired()],
    coerce=int
    )
    power = IntegerField('Power', validators=[InputRequired(), NumberRange(min=0)])
    cylinder_capacity = IntegerField('Cylinder Capacity', validators=[InputRequired(), NumberRange(min=0)])
    value_vehicle = IntegerField('Value of Vehicle', validators=[InputRequired(), NumberRange(min=0)])
    type_fuel = SelectField(
    'Type of Fuel',
    choices=[('P', 'Petrol'), ('D', 'Diesel')],
    validators=[InputRequired()]
    )
    year_matriculation = IntegerField('Year of Matriculation', validators=[InputRequired(), NumberRange(min=1886, max=2018)])
    submit = SubmitField('Submit')