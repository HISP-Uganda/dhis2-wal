CREATE TRIGGER foo_notify_trigger
AFTER
UPDATE
  OR
INSERT ON foo FOR EACH ROW EXECUTE PROCEDURE notify_table_update();
CREATE OR REPLACE FUNCTION notify_table_update() RETURNS TRIGGER LANGUAGE PLPGSQL AS $$ BEGIN 
IF TG_OP = 'INSERT' THEN PERFORM pg_notify(
    TG_TABLE_NAME,
    (row_to_json(NEW)::jsonb - 'geometry')::text
  );
END IF;
IF TG_OP = 'UPDATE' THEN PERFORM pg_notify(
  TG_TABLE_NAME,
  (row_to_json(NEW)::jsonb - 'geometry')::text
);
END IF;
RETURN null;
END;
$$;