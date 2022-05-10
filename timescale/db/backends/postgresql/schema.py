from django.conf import settings
from django.db.backends.postgresql.schema import DatabaseSchemaEditor

from timescale.db.models.fields import TimescaleDateTimeField, TimescalePartitioningField


class TimescaleSchemaEditor(DatabaseSchemaEditor):
    sql_is_hypertable = 'SELECT * FROM timescaledb_information.hypertables WHERE hypertable_name = {table}'

    sql_assert_is_hypertable = (
            'DO $do$ BEGIN '
            'IF EXISTS ( '
            + sql_is_hypertable +
            ') '
            'THEN NULL; '
            'ELSE RAISE EXCEPTION {error_message}; '
            'END IF;'
            'END; $do$'
    )
    sql_assert_is_not_hypertable = (
            'DO $do$ BEGIN '
            'IF EXISTS ( '
            + sql_is_hypertable +
            ') '
            'THEN RAISE EXCEPTION {error_message}; '
            'ELSE NULL; '
            'END IF;'
            'END; $do$'
    )

    sql_drop_primary_key = 'ALTER TABLE {table} DROP CONSTRAINT {pkey}'

    sql_add_hypertable = (
        "SELECT create_hypertable("
        "{table}, {time_column_name}, {args})"
    )

    sql_set_chunk_time_interval = 'SELECT set_chunk_time_interval({table}, interval {interval})'
    sql_set_number_partitions = 'SELECT set_number_partitions({table}, {number_partitions});'

    def _assert_is_hypertable(self, model):
        """
        Assert if the table is a hyper table
        """
        table = self.quote_value(model._meta.db_table)
        error_message = self.quote_value("assert failed - " + table + " should be a hyper table")

        sql = self.sql_assert_is_hypertable.format(table=table, error_message=error_message)
        self.execute(sql)

    def _assert_is_not_hypertable(self, model):
        """
        Assert if the table is not a hyper table
        """
        table = self.quote_value(model._meta.db_table)
        error_message = self.quote_value("assert failed - " + table + " should not be a hyper table")

        sql = self.sql_assert_is_not_hypertable.format(table=table, error_message=error_message)
        self.execute(sql)

    def _drop_primary_key(self, model):
        """
        Hypertables can't partition if the primary key is not
        the partition column.
        So we drop the mandatory primary key django creates.
        """
        db_table = model._meta.db_table
        table = self.quote_name(db_table)
        pkey = self.quote_name(f'{db_table}_pkey')

        sql = self.sql_drop_primary_key.format(table=table, pkey=pkey)

        self.execute(sql)

    def _create_args(self, **kwargs):
        args = ""
        for i, (key, value) in enumerate(kwargs.items()):
            args += key
            args += " => "
            args += value
            if i != len(kwargs) - 1:
                args += ","
        return args

    def _create_hypertable(self, model, time_field, partition_field=None, should_migrate=False):
        """
        Create the hypertable with the partition column being the field.
        """
        # assert that the table is not already a hypertable
        self._assert_is_not_hypertable(model)

        # drop primary key of the table
        self._drop_primary_key(model)

        
        table = self.quote_value(model._meta.db_table)
        time_column_name = self.quote_value(time_field.column)
        migrate = "true" if should_migrate else "false"

        args = {}
        args['chunk_time_interval'] = 'interval ' + self.quote_value(time_field.interval)
        args['migrate'] = migrate
        if partition_field is not None:
            args['partitioning_column'] = self.quote_value(partition_field.column)
            args['number_partitions'] = partition_field.number_partitions
        
        args_str = self._create_args(**args)

        if should_migrate and getattr(settings, "TIMESCALE_MIGRATE_HYPERTABLE_WITH_FRESH_TABLE", False):
            # TODO migrate with fresh table [https://github.com/schlunsen/django-timescaledb/issues/16]
            raise NotImplementedError()
        else:
            sql = self.sql_add_hypertable.format(
                table=table, time_column_name=time_column_name, args=args_str
            )
            self.execute(sql)

    def _set_chunk_time_interval(self, model, field):
        """
        Change time interval for hypertable
        """
        # assert if already a hypertable
        self._assert_is_hypertable(model)

        table = self.quote_value(model._meta.db_table)
        interval = self.quote_value(field.interval)

        sql = self.sql_set_chunk_time_interval.format(table=table, interval=interval)
        self.execute(sql)
    
    def _set_chunk_number_partitions(self, model, field):
        """
        Change number partition for hypertable
        """
        # assert if already a hypertable
        self._assert_is_hypertable(model)

        table = self.quote_value(model._meta.db_table)
        number_partitions = self.quote_value(field.number_partitions)

        sql = self.sql_set_number_partitions.format(table=table, number_partitions=number_partitions)
        self.execute(sql)

    def create_model(self, model):
        super().create_model(model)

        # scan if any field is of instance `TimescaleDateTimeField`
        time_field = None
        partition_field = None
        for field in model._meta.local_fields:
            if isinstance(field, TimescaleDateTimeField):
                time_field = field
            elif isinstance(field, TimescalePartitioningField):
                partition_field = field
        if time_field is not None:
            # create hypertable, with the field as partition column
            self._create_hypertable(model, time_field=time_field, partition_field=partition_field)

    def add_field(self, model, field):
        super().add_field(model, field)

        # scan if any field is of instance `TimescaleDateTimeField`
        time_field = None
        partition_field = None
        for field in model._meta.local_fields:
            if isinstance(field, TimescaleDateTimeField):
                time_field = field
            elif isinstance(field, TimescalePartitioningField):
                partition_field = field

        # check if this field is type `TimescaleDateTimeField`
        if isinstance(field, TimescaleDateTimeField):
            # migrate existing table to hypertable
            self._create_hypertable(model, time_field=field, partition_field=partition_field, should_migrate=True)
        elif isinstance(field, TimescalePartitioningField):
            self._create_hypertable(model, time_field=time_field, partition_field=field, should_migrate=True)

    def alter_field(self, model, old_field, new_field, strict=False):
        super().alter_field(model, old_field, new_field, strict)

        # scan if any field is of instance `TimescaleDateTimeField`
        time_field = None
        partition_field = None
        for field in model._meta.local_fields:
            if isinstance(field, TimescaleDateTimeField):
                time_field = field
            elif isinstance(field, TimescalePartitioningField):
                partition_field = field

        #  check if old_field is not type `TimescaleDateTimeField` and new_field is
        if not isinstance(old_field, TimescaleDateTimeField) and isinstance(new_field, TimescaleDateTimeField):
            # migrate existing table to hypertable
            self._create_hypertable(model, time_field=new_field, partition_field=field, should_migrate=True)
        # check if old_field and new_field is type `TimescaleDateTimeField` and `interval` is changed
        elif isinstance(old_field, TimescaleDateTimeField) and isinstance(new_field, TimescaleDateTimeField) \
                and old_field.interval != new_field.interval:
            # change chunk-size
            self._set_chunk_time_interval(model, new_field)

        #  check if old_field is not type `TimescalePartitioningField` and new_field is
        if not isinstance(old_field, TimescalePartitioningField) and isinstance(new_field, TimescalePartitioningField):
            # migrate existing table to hypertable
            self._create_hypertable(model, time_field=time_field, partition_field=new_field, should_migrate=True)
        # check if old_field and new_field is type `TimescalePartitioningField` and `interval` is changed
        elif isinstance(old_field, TimescalePartitioningField) and isinstance(new_field, TimescalePartitioningField) \
                and old_field.number_partitions != new_field.number_partitions:
            # change number partitions
            self._set_chunk_number_partitions(model, new_field)
