package com.cloudwick.hadoop.geolocation;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;

public class GeoLocation {
	DatabaseReader reader;
	
	public GeoLocation(String fileURI) throws IOException{
		reader = new DatabaseReader.Builder(new File(fileURI)).build();
		}
	
	public String getLocation(String ipAddress) {
		
		try {
		CityResponse response;
		
		response = reader.city(InetAddress.getByName(ipAddress));
		String city = response.getCity().getName();
		String country = response.getCountry().getName();
		StringBuilder result = new StringBuilder();
		
		if(city != null && city.length()!=0)
		{
			result.append(city);
			result.append(" ,");
		}
		
		if(country != null && country.length()!=0)
		result.append(country);
		
		String finalResult = result.toString();
		
		return finalResult;
		
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (GeoIp2Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return ipAddress;
	}
	
}
