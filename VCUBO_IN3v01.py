import streamlit as st
import pandas as pd
import numpy as np
import psycopg2
import time
import io

st.set_page_config(page_title='Project import - vcubo', page_icon='favicon.png',menu_items={'Get Help': 'https://vcubo.co/contact','Report a bug': "https://vcubo.co/contact",'About': " Unbiased risk ananlysis. *vcubo*"})


def check_password():
    """Returns `True` if the user had a correct password."""

    def password_entered():
        """Checks whether a password entered by the user is correct."""
        if (
            st.session_state["username"] in st.secrets["passwords"]
            and st.session_state["password"]
            == st.secrets["passwords"][st.session_state["username"]]
        ):
            st.session_state["password_correct"] = True
            st.session_state.com_id = st.session_state["username"] # store user for com_id field
            del st.session_state["password"]  # don't store username + password
            del st.session_state["username"]
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        h1,h2,h3 = st.columns((1,3,1))
        with h3: st.image('vcubo_betaW.png', width=100)
        with h1: st.subheader('LOG IN')
        # First run, show inputs for username + password.
        st.text_input("Username", on_change=password_entered, key="username")
        st.text_input(
            "Password", type="password", on_change=password_entered, key="password"
        )
        return False
    elif not st.session_state["password_correct"]:
        h1,h2,h3 = st.columns((1,3,1))
        with h3: st.image('vcubo_betaW.png', width=100)
        with h1: st.subheader('LOG IN')
        # Password not correct, show input + error.
        st.text_input("Username", on_change=password_entered, key="username")
        st.text_input(
            "Password", type="password", on_change=password_entered, key="password"
        )
        st.error("Incorrect user or password 😕")
        return False
    else:
        # Password correct.
        return True


if check_password():

    @st.cache(allow_output_mutation=True), hash_funcs={"_thread.RLock": lambda _: None})
    def init_connection():
        return psycopg2.connect(**st.secrets["postgres_prod"])

    @st.cache(ttl=600)
    def run_query(query):
        with conn2.cursor() as cur:
            cur.execute(query)
            conn2.commit()
    h1,h2,h3 = st.columns((3,3,1))

    with h3: st.image('vcubo_betaW.png', width=100)
    with h1: st.subheader('PROJECT IMPORT')

    conn2 = init_connection()
    cur = conn2.cursor()

    @st.cache(ttl=600)
    def write_blob(path_to_file, file_name):
        """ insert a BLOB into a table """
        # read data
        file = open(path_to_file, 'rb').read()
        # execute the INSERT statement
        cur.execute("INSERT INTO vcubo_templates(template_file, template_name) " +
                "VALUES(%s, %s)",
                (psycopg2.Binary(file), file_name))
        # commit the changes to the database
        conn2.commit()
        # close the communication with the PostgresQL database
        #cur.close()



    def read_blob(file):
        cur.execute(""" SELECT template_file, template_name FROM vcubo_templates WHERE template_name= %s """, (file,))
        blob = cur.fetchone()
        #cur.close()
        return bytes(blob[0])


    @st.cache(ttl=600)
    def upload_blob(project_id, uploaded_file, company_id):
        """ insert a BLOB into a table """
        # read data
        file = uploaded_file
        # execute the INSERT statement
        cur.execute("INSERT INTO pr_imported(l1_id, project_file, com_id) " +
                "VALUES(%s, %s, %s)",
                (project_id, psycopg2.Binary(file), company_id))
        # commit the changes to the database
        conn2.commit()
        # close the communication with the PostgresQL database
        #cur.close()

    def read_uploaded(project_id):
        cur.execute(""" SELECT l1_id, project_file FROM pr_imported WHERE l1_id= %s """, (project_id,))
        blob = cur.fetchone()
        #cur.close()
        return bytes(blob[1])

    #with st.expander('TEMPLATES UPLOADER'):

    #    st.session_state.path_to_file =st.text_input('File path:')
    #    st.session_state.template_name =st.text_input('Template name:')

    #    upload_template = st.button('UPLOAD TEMPLATE')
    #    if upload_template:
    #        write_blob(st.session_state.path_to_file, st.session_state.template_name)


    #st.subheader('DOWNLOAD TEMPLATE')
    #with st.expander('', expanded=False):
        #files_dict = {'LOGO':'vcubo_logo.png', 'PROJECT IMPORT TEMPLATE':'vcubo_project_import.xlsm'}
        #file_sel = st.selectbox('DOWNLOAD FILE', ['PROJECT IMPORT TEMPLATE'] )
        #st.download_button('DOWNLOAD TEMPLATE',
            #data=read_blob(files_dict[file_sel]),
            #file_name=files_dict[file_sel])

        #st.markdown('***')
        #decrypted =read_blob('vcubo_project_import.xlsm')
        #st.write(type(decrypted))
        #to_read = io.BytesIO()
        #to_read.write(decrypted)
        #st.write(to_read)
        #to_read.seek(0)
        #test_excel = pd.read_excel(to_read, sheet_name=2, header=0)
        #preview = st.button('PREVIEW')
        #if preview:
        #    test_excel
        #st.dataframe(read_blob('vcubo_project_import.xlsx'))
    st.download_button('DOWNLOAD IMPORT TEMPLATE',
        data=read_blob('vcubo_project_import.xlsm'),
        file_name='vcubo_project_import.xlsm')

    #st.subheader('IMPORT PROJECT DATA')
    #with st.expander('IMPORT PROJECT FILE', expanded=True):
    uploaded_file = st.file_uploader('SELECT FILE TO IMPORT', type='xlsm')

    st.session_state.project_id = st.text_input('PROJECT ID:')
    st.session_state.project_desc = st.text_area('BRIEF PROJECT DESCRIPTION', max_chars=200)
    st.session_state.pr_ids_main = pd.read_sql(f"SELECT l1_id  FROM pr_main", conn2)
    st.session_state.pr_ids_upl = pd.read_sql(f"SELECT l1_id  FROM pr_imported", conn2)
    if st.session_state.project_id =='':
        st.warning(f'ENTER UNIQUE PROJECT ID')
    elif st.session_state.project_id in st.session_state.pr_ids_main['l1_id'].tolist():
        st.warning(f'PROJECT ID {st.session_state.project_id} ALREADY EXISTS IN MAIN DATABASE')
    elif st.session_state.project_id in st.session_state.pr_ids_upl['l1_id'].tolist():
        st.warning(f'PROJECT ID {st.session_state.project_id} ALREADY IMPORTED (WAITING FOR VALIDATION)')
    else:
        upload_project_file = st.button('UPLOAD PROJECT')
        #type(uploaded_file)=="<class 'NoneType'>"
        st.warning(f'pre-uploaded file type: {type(uploaded_file)}')
        if (upload_project_file and type(uploaded_file)!='NoneType'):
            upload_blob(st.session_state.project_id, uploaded_file.getvalue(), st.session_state.com_id)
            time.sleep(2)
            uploaded_bytes = read_uploaded(st.session_state.project_id)
            uploaded_io = io.BytesIO()
            uploaded_io.write(uploaded_bytes)
            uploaded_io.seek(0)
            uploaded_df = pd.read_excel(uploaded_io, sheet_name='IMPORT')
            st.write(uploaded_df)
