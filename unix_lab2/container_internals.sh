#!/bin/sh
container_code=$HOSTNAME
counter=1
shared_dir=/app/shared
mkdir -p $shared_dir

lock_file=$shared_dir/.lockfile
touch $lock_file

exec 9>"$lock_file" # Ставим в соответствие файлу lockfile дескриптор 9

while :; do
	created=""
	flock -x 9
	i=1
	while [ $i -le 999 ]; do
		filename=$(printf %03d $i)
		path=$shared_dir/$filename
		if [ ! -e "$path" ]; then
			counter=$((counter+1))
            touch $path
			echo "$container_code $counter" > "$path"
			created="$filename"
			break
		fi
		i=$((i+1))
	done
	
	flock -u 9
	
	if [ -n "$created" ]; then
		sleep 1
		rm "$shared_dir/$created"
		sleep 1
	else
		sleep 2
	fi
done
