-- Since msyql doesn't provide equivalent features for postgres check
-- constraints, we can only mimic it with triggers.

DROP FUNCTION IF EXISTS _substr_count;
DROP PROCEDURE IF EXISTS _directories_check_constraints_proc;
DROP TRIGGER IF EXISTS directories_pre_insert_trigger;
DROP TRIGGER IF EXISTS directories_pre_update_trigger;

CREATE FUNCTION _substr_count(
    x VARCHAR(300),
    delim VARCHAR(300)
)
RETURNS INT
DETERMINISTIC
BEGIN
RETURN (LENGTH(x) - LENGTH(REPLACE(x, delim, ''))) / LENGTH(delim);
END;

CREATE PROCEDURE _directories_check_constraints_proc(
    IN user_id VARCHAR(30),
    IN name VARCHAR(300),
    IN parent_user_id VARCHAR(30),
    IN parent_name VARCHAR(300)
)
BEGIN
    -- Non-null versions, used in the concat() function, because concat()
    -- returns null if any of its args is null.
    SET @user_id = ifnull(user_id, 'null');
    SET @name = ifnull(name, 'null');
    SET @parent_user_id = ifnull(parent_user_id, 'null');
    SET @parent_name = ifnull(parent_name, 'null');

    -- CheckConstraint(
    --     'user_id = parent_user_id',
    --     name='directories_match_user_id',
    -- ),
    IF user_id != parent_user_id THEN
        SET @msg = concat(
                'Constraint directories_match_user_id violated: user_id = "',
                @user_id,
                '", but parent_user_id = "',
                @parent_user_id,
                '"'
        );
        SIGNAL sqlstate '45000' SET message_text = @msg;

    -- # Assert that parent_name is a prefix of name.
    -- CheckConstraint(
    --     "position(parent_name in name) != 0",
    --     name='directories_parent_name_prefix',
    -- ),
    ELSEIF LOCATE(parent_name, name) = 0 THEN
        SET @msg = concat(
                'Constraint directories_parent_name_prefix violated: name = "',
                @name,
                '", parent_name = "',
                @parent_name,
                '"'
        );
        SIGNAL sqlstate '45000' SET message_text = @msg;

    -- # Assert that all directories begin or end with '/'.
    -- CheckConstraint(
    --     "left(name, 1) = '/'",
    --     name='directories_startwith_slash',
    -- ),

    ELSEIF SUBSTRING(name, 1, 1) != '/' THEN
        SET @msg = concat(
                'Constraint directories_startwith_slash violated: name = "',
                @name,
                '"'
        );
        SIGNAL sqlstate '45000' SET message_text = @msg;

    -- CheckConstraint(
    --     "right(name, 1) = '/'",
    --     name='directories_endwith_slash',
    -- ),

    ELSEIF SUBSTRING(name, -1) != '/' THEN
        SET @msg = concat(
                'Constraint directories_endwith_slash violated: name = "',
                @name,
                '"'
        );
        SIGNAL sqlstate '45000' SET message_text = @msg;

    -- # Assert that the name of this directory has one more '/' than its parent.
    -- CheckConstraint(
    --     "length(regexp_replace(name, '[^/]+', '', 'g')) - 1"
    --     "= length(regexp_replace(parent_name, '[^/]+', '', 'g'))",
    --     name='directories_slash_count',
    -- ),
    ELSEIF _substr_count(name, '/') != _substr_count(parent_name, '/') + 1 THEN
        SET @msg = concat(
                'Constraint directories_slash_count violated: name = "',
                @name,
                '", parent_name = "',
                @parent_name,
                '"'
        );
        SIGNAL sqlstate '45000' SET message_text = @msg;

    -- # Assert that parent_user_id is NULL iff parent_name is NULL.  This should
    -- # be true only for each user's root directory.
    -- CheckConstraint(
    --     ''.join(
    --         [
    --             '(parent_name IS NULL AND parent_user_id IS NULL)'
    --             ' OR ',
    --             '(parent_name IS NOT NULL AND parent_user_id IS NOT NULL)'
    --         ],
    --     ),
    --     name='directories_null_user_id_match',
    -- ),
    ELSEIF NOT ((parent_name is NULL AND parent_user_id is NULL)
            OR  (parent_name is not NULL AND parent_user_id is not NULL)) THEN
        SET @msg = concat(
                'Constraint directories_null_user_id_match violated: parent_name = "',
                @parent_name,
                '", parent_user_id = "',
                @parent_user_id,
                '"'
        );
        SIGNAL sqlstate '45000' SET message_text = @msg;

    END IF;
END;


CREATE TRIGGER directories_pre_insert_trigger
BEFORE INSERT on directories
FOR EACH ROW
BEGIN
    CALL _directories_check_constraints_proc(
        NEW.user_id,
        NEW.name,
        NEW.parent_user_id,
        NEW.parent_name
    );
END;

CREATE TRIGGER directories_pre_update_trigger
BEFORE UPDATE on directories
FOR EACH ROW
BEGIN
    CALL _directories_check_constraints_proc(
        NEW.user_id,
        NEW.name,
        NEW.parent_user_id,
        NEW.parent_name
    );
END;
