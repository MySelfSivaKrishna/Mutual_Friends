package com.mapreduce_code;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.custom_writables.Friend;
import com.custom_writables.FriendPair;
import com.custom_writables.FriendsArray;

public class MutualFriendsMapper extends Mapper<LongWritable, Text, com.custom_writables.FriendPair, com.custom_writables.FriendsArray> {

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, FriendPair, FriendsArray>.Context context)
			throws IOException, InterruptedException {
		String input = value.toString();
		try {
			HashMap<Friend, ArrayList<Friend>> key_list_value = parseappdata(context, input);
			Friend indiv = null;
			ArrayList<Friend > list_friends = null;
			for(Friend each:key_list_value.keySet() ){
				indiv = each;
			}
			for(ArrayList<Friend > eachlist:key_list_value.values() ){
				list_friends = eachlist;
			}
			Friend[] friend_list= Arrays.copyOf(list_friends.toArray(), list_friends.toArray().length, Friend[].class);
			FriendsArray farray = new FriendsArray(Friend.class, friend_list);

			for(Friend each_one:list_friends){
				FriendPair fp = new FriendPair(indiv, each_one);
				
				
				context.write(fp, farray);
				}

		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 
		
		
	}
	
	static HashMap<Friend, ArrayList<Friend>> parseappdata(Context context,String in) throws ParseException  {
		String head = in.substring(0, in.indexOf("[")-1);
		String friends = in.substring(in.indexOf("["));
		Friend friend;
		ArrayList<Friend> friendlist= new ArrayList<>();
		
		JSONParser parser = new JSONParser();
		JSONObject root = (JSONObject) parser.parse(head);
		friend = new Friend(new IntWritable((int)root.get("id")),new Text((String)root.get("name")),new Text((String)root.get("hometown")));
		JSONArray tail = (JSONArray) parser.parse(friends);
		for(Object each: tail){
			Friend indivi_friend;
			JSONObject individual = (JSONObject) each;
			indivi_friend = new Friend(new IntWritable((int)individual.get("id")),new Text((String)individual.get("name")),new Text((String)individual.get("hometown")));
            friendlist.add(indivi_friend);
            
		}
		HashMap<Friend, ArrayList<Friend>> return_object = new HashMap<>();
		return_object.put(friend, friendlist);
		
		
		return return_object;

      
    }

   

}
