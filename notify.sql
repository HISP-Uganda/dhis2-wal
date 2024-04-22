CREATE OR REPLACE FUNCTION notify_new_order() RETURNS trigger AS $BODY$
    BEGIN
        PERFORM pg_notify('new_data_value', row_to_json(NEW)::text);
        RETURN NULL;
    END;
$BODY$ LANGUAGE plpgsql VOLATILE COST 100;


CREATE TRIGGER new_data_value AFTER
INSERT ON "datavalue"
FOR EACH ROW EXECUTE PROCEDURE notify_new_order();


CREATE TRIGGER update_data_value AFTER
UPDATE ON "datavalue"
FOR EACH ROW EXECUTE PROCEDURE notify_new_order();