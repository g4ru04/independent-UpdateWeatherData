package CSI;

//20170904 Ben 

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStream;

import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.google.gson.Gson;

public class UpdateWeatherData {
	private static final Logger logger = LogManager.getLogger(UpdateWeatherData.class);
	private static String dbURLbatch ;
	private static String dbUserName ;
	private static String dbPassword ;
	private static String tmpDIR = "temporary";
	private static String projectPath ;
	
	private static String needSP = "sp_insert_data_predict_weather";
	private static String needTB = "tb_data_predict_weather";
	
	
	public static void main(String[] args){
		logger.info("�}�Ҥu�@-�۰ʧ�s������H��-�Ѯ�w�����");
		try {
			String sysString = UpdateWeatherData.class.getProtectionDomain().getCodeSource().getLocation().getPath(); 
			projectPath = sysString.split(sysString.split("/")[sysString.split("/").length - 1])[0];
			String todayStr = new SimpleDateFormat("yyyyMMdd").format(new Date());
			getConfigs();
			
			logger.info("���O> �۰ʧ�s������H��-�Ѯ�w�����[ "+todayStr+" ] �i��: �U���ɮ�(�O�_��,�s�_��)(1/3)");
			
			File destFolder = new File(projectPath+"/"+tmpDIR);
			logger.debug("�ɮ׼Ȧs��m: '"+destFolder.getAbsolutePath()+"'");
			destFolder.mkdirs();
			
			String taipeiCityUrl = "http://opendata.cwb.gov.tw/opendataapi?dataid=F-D0047-061&authorizationkey=CWB-2827DD82-A5F4-44AC-819B-48401D3117DA";
			String newTaipeiUrl = "http://opendata.cwb.gov.tw/opendataapi?dataid=F-D0047-069&authorizationkey=CWB-2827DD82-A5F4-44AC-819B-48401D3117DA";
			
			//�U���O�_�����
			if(isValidURL(taipeiCityUrl)){
				downlodFileSuccess(
						taipeiCityUrl,
					destFolder+"/"+todayStr+"_F-D0047-061.xml"
				);
			}
			//�U���s�_�����
			if(isValidURL(newTaipeiUrl)){
				downlodFileSuccess(
						newTaipeiUrl,
					destFolder+"/"+todayStr+"_F-D0047-069.xml"
				);
			}
			
			logger.info("���O> �۰ʧ�s������H��-�Ѯ�w�����[ "+todayStr+" ] �i��: �פJ��Ʈw(2/3)");
			
			//��s�O�_�����
			if(isValidURL(taipeiCityUrl)){
				logger.info("�O�_����ƶפJ���C");
				parseUpdate(destFolder+"/"+todayStr+"_F-D0047-061.xml");
			}
			//��s�s�_�����
			if(isValidURL(newTaipeiUrl)){
				logger.info("�s�_����ƶפJ���C");
					parseUpdate(destFolder+"/"+todayStr+"_F-D0047-069.xml");
			}
			
			logger.info("���O> �۰ʧ�s������H��-�Ѯ�w�����[ "+todayStr+" ] �i��: �R���Ȧs��(3/3)");
			deleteFolder(destFolder.getAbsolutePath());
			
			logger.info("�����u�@-�۰ʧ�s������H��-�Ѯ�w�����");
			
		} catch (Exception e) {
			logger.info("��s�Ѯ�w����Ʋ��`�A����{�� : "+e.toString());
			e.printStackTrace();
		}
	}
	public static void parseUpdate(String path) throws Exception{
		String sysString = UpdateWeatherData.class.getProtectionDomain().getCodeSource().getLocation().getPath(); 
		projectPath = sysString.split(sysString.split("/")[sysString.split("/").length - 1])[0];
		String[] positionWeNeed_arr = {"�x�_����s��","�x�_���j�w��","�x�_���Q�s��","�x�_�����s��","�x�_�������",
				"�x�_���n���","�x�_���H�q��","�x�_��������","�x�_���j�P��","�x�_���h�L��","�x�_���_���","�s�_���H����","�s�_���s����",
				"�s�_�����M��","�s�_���éM��","�s�_���T����","�s�_���s����","�s�_��Ī�w��","�s�_���g����","�s�_���O����","�x�_���U�ذ�"};
		List<String> positionWeNeed = new ArrayList<String>();
		@SuppressWarnings("unused")
		String predict_time="",county="";
		for(int i=0;i<positionWeNeed_arr.length;i++){
			positionWeNeed.add(positionWeNeed_arr[i].substring(3, 6));
		}
		JSONArray batchElement= new JSONArray();
		try {
			
			File f = new File(path);
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			Document doc = builder.parse(f);
			NodeList nl = doc.getElementsByTagName("locations");
			
			Element dataSetNode = (Element)nl.item(0);
			
			predict_time = outDateFormat(inDateFormat(doc.getElementsByTagName("sent").item(0).getFirstChild().getNodeValue().trim()));
			county = dataSetNode.getElementsByTagName("locationsName").item(0).getFirstChild().getNodeValue().trim();
			for(int i=0;i< dataSetNode.getElementsByTagName("location").getLength() ;i++){
				String region="",geocode="",lat="",lng="";
				Element locationNode = (Element)dataSetNode.getElementsByTagName("location").item(i);
				
				region =  getTagVal(locationNode,"locationName");
				geocode =  getTagVal(locationNode,"geocode");
				lat =  getTagVal(locationNode,"lat");
				lng =  getTagVal(locationNode,"lon");
				
				if(!positionWeNeed.contains(region)){
					continue ;
				}
				for(int j=0;j< locationNode.getElementsByTagName("weatherElement").getLength() ;j++){
					Element weatherNode = (Element)locationNode.getElementsByTagName("weatherElement").item(j);
					String climate_type = getTagVal(weatherNode,"elementName"),
							climate_value = "",climate_unit = "",
							climate_des_name = "",climate_des_value = "",
							climate_day = "",first_climate_time = "",second_climate_time = "",third_climate_time = "";
					
					for(int k=0;k< weatherNode.getElementsByTagName("time").getLength() ;k++){
						Element dataNode = (Element)weatherNode.getElementsByTagName("time").item(k);
						
						if("T".equals(climate_type)||"Wx".equals(climate_type)){
							if("T".equals(climate_type)){
								climate_day = new SimpleDateFormat("yyyy-MM-dd").format(inDateFormat(getTagVal(dataNode,"dataTime")));
								Calendar cal = Calendar.getInstance();
								cal.setTime(inDateFormat(getTagVal(dataNode,"dataTime")));  
								first_climate_time = String.format("%02d",cal.get(Calendar.HOUR_OF_DAY));
								cal.add(Calendar.HOUR_OF_DAY, 1);
								second_climate_time = String.format("%02d",cal.get(Calendar.HOUR_OF_DAY));
								cal.add(Calendar.HOUR_OF_DAY, 1);
								third_climate_time = String.format("%02d",cal.get(Calendar.HOUR_OF_DAY));
								climate_value = getTagVal(dataNode,"value");
								climate_unit = getTagVal(dataNode,"measures");
								//insert thrice
							}else if("Wx".equals(climate_type)){
								climate_day = new SimpleDateFormat("yyyy-MM-dd").format(inDateFormat(getTagVal(dataNode,"startTime")));
								Calendar cal = Calendar.getInstance();
								cal.setTime(inDateFormat(getTagVal(dataNode,"startTime")));  
								first_climate_time = String.format("%02d",cal.get(Calendar.HOUR_OF_DAY));
								cal.add(Calendar.HOUR_OF_DAY, 1);
								second_climate_time = String.format("%02d",cal.get(Calendar.HOUR_OF_DAY));
								cal.add(Calendar.HOUR_OF_DAY, 1);
								third_climate_time = String.format("%02d",cal.get(Calendar.HOUR_OF_DAY));
								climate_value = getTagVal(dataNode,"value");
								climate_des_name = getTagVal(dataNode,"parameterName");
								climate_des_value = getTagVal(dataNode,"parameterValue");
								//insert thrice
							}
							
							for (int l=0;l<3;l++){
								Map<String,String> vo1 = new LinkedHashMap<String,String>();
								vo1.put("a", climate_day);

								if(l==0){
									vo1.put("b", first_climate_time);
								}else if(l==1){
									vo1.put("b", second_climate_time);
								}else if(l==2){
									vo1.put("b", third_climate_time);
								}
								
								vo1.put("c", climate_type);
								vo1.put("d", climate_value);
								vo1.put("e", climate_unit);
								vo1.put("f", climate_des_name);
								vo1.put("g", climate_des_value);
								vo1.put("h", geocode);
								vo1.put("i", region);
								vo1.put("j", lat);
								vo1.put("k", lng);
								vo1.put("l", predict_time);
								batchElement.put(vo1);
							}
							
						}
					}
				}
			}
			batchCMD("call sp_insert_data_predict_weather(?,?,?,?,?,?,?,?,?,?,?,?)",batchElement);
			
		} catch (Exception e) {
			logger.debug("�B�z��Ƶo�Ͳ��`: �ˬd�{���ɮ׮榡�O�_�ܧ�");
			throw new Exception(e.toString());
		}
	}
	
	public static void getConfigs(){
		try{
			String config_path = projectPath+"/config.properties";
			
			if(!(new File(config_path).exists())){
				logger.info("�]�w��: "+config_path+" ���s�b");
				return ;
			}
			BufferedReader reader = new BufferedReader(new FileReader(config_path));
			reader.readLine();
			String line = null; 
			while((line=reader.readLine())!=null){
				String item[] = line.split("=");
				
				if(item.length == 2 && (!"#".equals(line.substring(0,1)))){
					tmpDIR = item[0].trim().equals("tmpDIR") ? item[1].trim() : tmpDIR ;
					dbURLbatch = item[0].trim().equals("dbURL") ? item[1].trim() : dbURLbatch + "?useUnicode=true&characterEncoding=utf-8&useSSL=false&useServerPrepStmts=false&rewriteBatchedStatements=true" ;
					dbUserName = item[0].trim().equals("dbUserName") ? item[1].trim() : dbUserName ;
					dbPassword = item[0].trim().equals("dbPassword") ? item[1].trim() : dbPassword ;
				}
			}
			reader.close();
		}catch(Exception e){
			logger.debug("�]�w��Ū�����`");
//			e.printStackTrace();
		}
	}
	
	public static boolean batchCMD(String sp,JSONArray requestElements) throws Exception{
		int batchCount = 0;
		Connection conn;
		PreparedStatement psts;
		try{
	        Class.forName("com.mysql.jdbc.Driver");  
	        conn = (Connection) DriverManager.getConnection(dbURLbatch, dbUserName,dbPassword);  
	        conn.setAutoCommit(false);
	        psts = conn.prepareStatement(sp);  
	        Date begin=new Date();
	        for( int i=0 ; requestElements!=null && i<requestElements.length() ; i++ ){
	        	JSONObject element = requestElements.getJSONObject(i);
	        	int j=0;
				for (Iterator<String> iter = element.keys(); iter.hasNext();) {
			        String key = (String)iter.next();
			        psts.setString(++j, element.getString(key));
				}
				psts.addBatch();
	            if((++batchCount)%(11*10000)==0){
	            	
	            	logger.info("batch��Ʈw�B�z��ƶq�F "+batchCount+" ��");
	            	psts.executeBatch();
		        	conn.commit();
		        	psts.clearBatch();
		        	
		        }
			}
	        psts.executeBatch();
	        conn.commit();
	        Date end=new Date();

	        logger.info("�@�B�z "+batchCount+" ����ơA�Ӯ�: "+(end.getTime()-begin.getTime())+" ms");
	        conn.close();  
	        return true;
		}catch(Exception e){
			logger.debug("��Ʈw�s�u���`�A�ˬd��Ʈw sp: "+needSP+" table: "+needTB);
			throw new Exception("�妸MysqlCMD����: "+e.toString());
		}
    }  
	
	public static String null2str(Object object) {
		if("null".equals(object)){
			object = null;
		}
		return object == null ? "" : object.toString().trim();
	}
	
	public static String JSONStringify(Object object){
		Gson gson = new Gson();
		return  gson.toJson(object);
	}
	
	public static boolean isValidURL(String urlStr) {
		URL url;
		try {
			url = new URL(urlStr);
			InputStream in = url.openStream();
			in.close();
		} catch (Exception e1) {
			e1.printStackTrace();
			logger.info("URL�L��: " + urlStr);
			url = null;
			return false;
		}
		return true;
	}
	
	public static boolean downlodFileSuccess(String downloadStr, String downloadPosition) {
		try {
			FileUtils.copyURLToFile(new URL(downloadStr), new File(downloadPosition));
		} catch (Exception e) {
			logger.info("�U������: " + e.toString());
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	public static boolean deleteFolder(String folder) {
		
		File file = new File(folder);
		if (!file.exists()) {
			return true;
		}
		if (!file.isDirectory()) {
			return true;
		}
		String[] tempList = file.list();
		File temp = null;
		for (int i = 0; i < tempList.length; i++) {
			if (folder.endsWith(File.separator)) {
				temp = new File(folder + tempList[i]);
			} else {
				temp = new File(folder + File.separator + tempList[i]);
			}
			if (temp.isFile()) {
				temp.delete();
			}
			if (temp.isDirectory()) {
				deleteFolder(folder + "/" + tempList[i]);
				delFolder(folder + "/" + tempList[i]);
			}
		}
		return true;
		
	}

	public static void delFolder(String folderPath) {
		
		try {
			deleteFolder(folderPath);
			String filePath = folderPath;
			filePath = filePath.toString();
			java.io.File myFilePath = new java.io.File(filePath);
			myFilePath.delete();
		} catch (Exception e) {
			logger.debug("�M�Ÿ�Ƨ��ާ@�X�� ");
			e.printStackTrace();
		}
		
	}
	
	public static Element getChilds(Element ele,String tagName){
		
		return (Element) ele.getElementsByTagName(tagName).item(0);
		
	}
	
	public static String getTagVal(Element ele,String tagName){
		
		if(ele==null){
			return "";
		}else{
			return ele.getElementsByTagName(tagName)
				.item(0)
				.getFirstChild()
				.getNodeValue()
				.trim();
		}
		
	}
	
	public static Date inDateFormat(String input){
		
		DateFormat outFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		try {
			return outFormat.parse(input.replaceAll("\\+08:00","").replaceAll("T"," "));
		} catch (ParseException e) {
			e.printStackTrace();
			return new Date();
		}
		
	}
	
	public static String outDateFormat(Date dat){
		
		DateFormat outFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return outFormat.format(dat);
		
	}
}
