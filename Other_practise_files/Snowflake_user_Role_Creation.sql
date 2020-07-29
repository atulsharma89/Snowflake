1. To set your current role context to SECURITYADMIN (if you have that role):

          Use role SECURITYADMIN;

2. Create a new role you want to assign to the new user if the role does not already exist:

         CREATE ROLE New_Role COMMENT = 'This is a new role';

3. Grant privileges to the new role:

         Grant all on database sales to new_role;

         Or

         Grant select on sales.public.lineitem to new_role;

         Or

         Grant select, insert, delete on sales.public.lineitem to new_role;

4. Create a user:

         CREATE USER New_User_Id   -- e.g. Mike

               PASSWORD = '**********'

               COMMENT = 'This is a new user'                                -- Optional

               LOGIN_NAME = 'Mike_Login_Name'                         -- Optional

               DISPLAY_NAME = 'Mike_Display_Name'                  -- Optional

               FIRST_NAME = 'Mike'                                                -- Optional

               LAST_NAME = 'Smith'                                                -- Optional

               EMAIL = '<MSmith@gmail.com>'                             -- Optional

               DEFAULT_ROLE = 'new_role'                                    -- Optional

               DEFAULT_WAREHOUSE = 'demo_wh'                       -- Optional

               DEFAULT_NAMESPACE = '<Default Namespace>'   -- Optional

               MUST_CHANGE_PASSWORD = TRUE;                       -- Optional 

NOTE: All users by default have access to the PUBLIC role. In the above example, we are specifying new_role as the default role for the new user. 

5. Even though the default role for the new user has been set to new_role, the owner of the new_role has to explicitly grant the new_role to the new user as follows:

Grant role <new_role> to user <new_user_id>;