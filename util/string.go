package util

func RemoveString(slice []string, target string) []string {
	for i, s := range slice {
		if s == target {
			return append(slice[:i], slice[i+1:]...)
		}
	}
	return slice // 如果没找到，原样返回
}
