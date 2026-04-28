import streamlit as st
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, text
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from google.oauth2 import service_account
import gspread
import datetime
import uuid
import leafmap.foliumap as leafmap
from streamlit_folium import st_folium

# --- 1. CONFIGURATION ---
CSV_URL = "https://raw.githubusercontent.com/chakrit39/event2023/refs/heads/main/office_seq.csv"
DB_CONFIG = "postgresql://username:password@host:port/database"
SPREADSHEET_ID = 'YOUR_GOOGLE_SHEET_ID'
DRIVE_FOLDER_ID = 'YOUR_GOOGLE_DRIVE_FOLDER_ID'

# --- 2. CORE FUNCTIONS ---

@st.cache_data
def load_office_data():
    return pd.read_csv(CSV_URL)

def get_google_creds():
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    info = st.secrets["gcp_service_account"]
    credentials = service_account.Credentials.from_service_account_info(
        info, scopes=scopes)
    return credentials

def upload_images_to_drive(uploaded_files, creds):
    drive_service = build('drive', 'v3', credentials=creds)
    ids = []
    for file in uploaded_files:
        file_metadata = {'name': file.name, 'parents': [DRIVE_FOLDER_ID]}
        media = MediaIoBaseUpload(file, mimetype=file.type, resumable=True)
        res = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        ids.append(res.get('id'))
    return ",".join(ids)

def delete_drive_files(file_ids_str, creds):
    if not file_ids_str: return
    drive_service = build('drive', 'v3', credentials=creds)
    for fid in file_ids_str.split(","):
        try: drive_service.files().delete(fileId=fid).execute()
        except: pass

# --- 3. UI LAYOUT ---

st.set_page_config(page_title="Geo-Data Collector", layout="wide")
st.title("📍 ระบบบันทึกข้อมูลเชิงพื้นที่")

try:
    df_office = load_office_data()

    # SECTION 1: ข้อมูลเบื้องต้น
    with st.expander("📝 ส่วนที่ 1: ข้อมูลสำนักงานที่ดิน", expanded=True):
        c1, c2 = st.columns([0.3,0.6])
        with c1:
            provinces = sorted(df_office['pro_name'].unique())
            sel_province = st.selectbox("เลือกจังหวัด", provinces)
        with c2:
            filtered_offices = df_office[df_office['pro_name'] == sel_province]
            sel_office = st.selectbox("เลือกสำนักงานที่ดิน", sorted(filtered_offices['office_name'].unique()))

    # SECTION 2: อัพโหลดและ Preview
    with st.expander("🗺️ ส่วนที่ 2: อัพโหลดและตรวจสอบแผนที่", expanded=True):
        up1, up2 = st.columns([1, 1])
        with up1:
            img_files = st.file_uploader("รูปภาพ (หลายไฟล์)", type=['jpg','png','jpeg'], accept_multiple_files=True)
        with up2:
            shp_file = st.file_uploader("Shapefile (.zip)", type=['zip'])

        if shp_file:
            gdf = gpd.read_file(shp_file)
            if gdf.crs != "EPSG:4326":
                indian_1975_with_shift = "+proj=utm +zone=47 +ellps=evrst30 +towgs84=204,837,295,0,0,0,0 +units=m +no_defs"
                gdf.crs = indian_1975_with_shift
                gdf = gdf.to_crs(epsg=4326)
            
            st.write(f"✅ พบข้อมูล {len(gdf)} features")
            
            # เลือก Index ที่ต้องการ
            selected_indices = st.selectbox(
                "เลือก Index ที่ต้องการบันทึก", 
                options=gdf.index.tolist()
            )
            
            if selected_indices != "":
                filtered_gdf = gdf.loc[selected_indices:selected_indices]
                #filtered_gdf = filtered_gdf.reset_index(drop=True)
                #st.dataframe(filtered_gdf)
                # แผนที่ภาพถ่ายดาวเทียม
                #m = leafmap.Map(google_map="SATELLITE", max_zoom=22, max_native_zoom=18)
                m = leafmap.Map(max_zoom=22) 
    
                # 2. เพิ่ม Google Satellite Layer แบบกำหนดค่า Zoom เอง
                # ใช้ URL มาตรฐานของ Google และกำหนด max_native_zoom
                google_satellite_url = "https://mt1.google.com/vt/lyrs=s&x={x}&y={y}&z={z}"
                
                m.add_tile_layer(
                    url=google_satellite_url,
                    name="Google Satellite",
                    attribution="Google",
                    max_zoom=22,           # ระดับที่อนุญาตให้ซูมในหน้าจอ (18-22 จะเป็นการขยายภาพ)
                    max_native_zoom=18     # ระดับซูมสูงสุดที่ Google มีภาพจริง (ปกติคือ 18 หรือ 19)
                )
                m.add_gdf(filtered_gdf, layer_name="Preview")
                m.zoom_to_gdf(filtered_gdf)
                st_folium(m, height=1000, use_container_width=True)
                
            else:
                st.warning("⚠️ โปรดเลือกอย่างน้อย 1 feature")

    # SECTION 3: บันทึกข้อมูล
    if st.button("🚀 ยืนยันและบันทึกข้อมูลทั้งหมด", use_container_width=True):
        if not (shp_file and img_files  and selected_indices):
            st.error("❌ ข้อมูลไม่ครบ: กรุณาใส่โน้ต เลือกรูปภาพ และเลือก Feature ในแผนที่")
        else:
            batch_id = str(uuid.uuid4())
            uploaded_drive_ids = None
            engine = create_engine(DB_CONFIG)
            
            try:
                creds = get_google_creds()
                bar = st.progress(0)
                
                # 1. Drive
                st.write("⏳ กำลังบันทึกรูปภาพ...")
                uploaded_drive_ids = upload_images_to_drive(img_files, creds)
                bar.progress(33)
                
                # 2. PostGIS
                st.write("⏳ กำลังบันทึกข้อมูลเชิงพื้นที่...")
                filtered_gdf['batch_id'] = batch_id
                with engine.begin() as conn:
                    filtered_gdf.to_postgis("survey_data", conn, if_exists='append', index=False)
                    res = conn.execute(text("SELECT id FROM survey_data WHERE batch_id = :bid"), {"bid": batch_id})
                    feature_ids_str = ",".join([str(r[0]) for r in res])
                bar.progress(66)
                
                # 3. Google Sheets
                st.write("⏳ กำลังบันทึกสรุปลง Google Sheet...")
                gc = gspread.authorize(creds)
                sheet = gc.open_by_key(SPREADSHEET_ID).sheet1
                sheet.append_row([
                    str(datetime.datetime.now()), sel_province, sel_office, 
                    uploaded_drive_ids, feature_ids_str, batch_id
                ])
                
                bar.progress(100)
                st.success(f"🎊 บันทึกข้อมูลสำเร็จ! (Batch ID: {batch_id})")
                st.balloons()

            except Exception as e:
                st.error(f"💥 เกิดข้อผิดพลาด: {e}")
                st.warning("🔄 เริ่มกระบวนการ Rollback ...")
                if uploaded_drive_ids:
                    delete_drive_files(uploaded_drive_ids, creds)
                with engine.begin() as conn:
                    conn.execute(text("DELETE FROM survey_data WHERE batch_id = :bid"), {"bid": batch_id})
                st.info("✅ ล้างข้อมูลที่ผิดพลาดเรียบร้อยแล้ว")

except Exception as e:
    st.error(f"ไม่สามารถโหลดข้อมูลเริ่มต้นได้: {e}")
