package com.mapreduce_code;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.hadoop.mapreduce.Reducer;

import com.custom_writables.Friend;
import com.custom_writables.FriendPair;
import com.custom_writables.FriendsArray;

public class MutualFriendsReducer extends Reducer<FriendPair, FriendsArray, FriendPair,FriendsArray> {

	@Override
	protected void reduce(FriendPair arg0, Iterable<FriendsArray> arg1,
			Reducer<FriendPair, FriendsArray, FriendPair, FriendsArray>.Context context)
			throws IOException, InterruptedException {
		HashMap<Friend, Integer> decison_maker = new HashMap<>();
		for(FriendsArray every_array: arg1){
			Friend[] list_of_friends = Arrays.copyOf(every_array.get(), every_array.get().length, Friend[].class);
			for(Friend every:list_of_friends){
				if(decison_maker.containsKey(every)){
					int temp = decison_maker.get(every) + 1;
					decison_maker.put(every,temp);
				}
				else{
					decison_maker.put(every,1);
				}
			}
			
		}
		ArrayList<Friend>  mutuallist= new ArrayList<>();
		for(Friend eachone:decison_maker.keySet()){
			
			if(decison_maker.get(eachone)>=2){
				mutuallist.add(eachone);
				
			}	
		}
		Friend[] mutual_array = (Friend[]) mutuallist.toArray();
		FriendsArray mutual_friends = new FriendsArray(Friend.class, mutual_array);
		
		
		context.write(arg0, mutual_friends);
		
	}

}
