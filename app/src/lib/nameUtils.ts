
export function normalizeName(name: string): string {
  const charMap: Record<string, string> = {
    'á': 'a', 'à': 'a', 'ä': 'a', 'â': 'a', 'ã': 'a', 'å': 'a', 'ā': 'a', 'ă': 'a', 'ą': 'a', 'ǎ': 'a',
    'Á': 'a', 'À': 'a', 'Ä': 'a', 'Â': 'a', 'Ã': 'a', 'Å': 'a', 'Ā': 'a', 'Ă': 'a', 'Ą': 'a', 'Ǎ': 'a',
    'é': 'e', 'è': 'e', 'ë': 'e', 'ê': 'e', 'ē': 'e', 'ė': 'e', 'ę': 'e', 'ě': 'e',
    'É': 'e', 'È': 'e', 'Ë': 'e', 'Ê': 'e', 'Ē': 'e', 'Ė': 'e', 'Ę': 'e', 'Ě': 'e',
    'í': 'i', 'ì': 'i', 'ï': 'i', 'î': 'i', 'ī': 'i', 'į': 'i', 'ı': 'i',
    'Í': 'i', 'Ì': 'i', 'Ï': 'i', 'Î': 'i', 'Ī': 'i', 'Į': 'i', 'İ': 'i',
    'ó': 'o', 'ò': 'o', 'ö': 'o', 'ô': 'o', 'õ': 'o', 'ø': 'o', 'ō': 'o', 'ő': 'o', 'ǫ': 'o',
    'Ó': 'o', 'Ò': 'o', 'Ö': 'o', 'Ô': 'o', 'Õ': 'o', 'Ø': 'o', 'Ō': 'o', 'Ő': 'o', 'Ǫ': 'o',
    'ú': 'u', 'ù': 'u', 'ü': 'u', 'û': 'u', 'ū': 'u', 'ů': 'u', 'ű': 'u', 'ų': 'u',
    'Ú': 'u', 'Ù': 'u', 'Ü': 'u', 'Û': 'u', 'Ū': 'u', 'Ů': 'u', 'Ű': 'u', 'Ų': 'u',
    'ý': 'y', 'ÿ': 'y', 'ŷ': 'y', 'Ý': 'y', 'Ÿ': 'y', 'Ŷ': 'y',
    'ç': 'c', 'ć': 'c', 'č': 'c', 'ĉ': 'c', 'ċ': 'c',
    'Ç': 'c', 'Ć': 'c', 'Č': 'c', 'Ĉ': 'c', 'Ċ': 'c',
    'đ': 'd', 'Đ': 'd',
    'ñ': 'n', 'ń': 'n', 'ň': 'n', 'ņ': 'n',
    'Ñ': 'n', 'Ń': 'n', 'Ň': 'n', 'Ņ': 'n',
    'š': 's', 'ś': 's', 'ş': 's',
    'Š': 's', 'Ś': 's', 'Ş': 's',
    'ž': 'z', 'ź': 'z', 'ż': 'z',
    'Ž': 'z', 'Ź': 'z', 'Ż': 'z',
    'ģ': 'g', 'Ģ': 'g',
    'ķ': 'k', 'Ķ': 'k',
    'ļ': 'l', 'Ļ': 'l',
  };
  
  return name.split('').map(c => charMap[c] || c).join('');
}


export function cleanNameForMatching(name: string): string {
  
  let cleaned = normalizeName(name);
  
  
  cleaned = cleaned.replace(/\s+(Jr\.?|Sr\.?|II|III|IV)$/i, '');
  
  
  cleaned = cleaned.replace(/'/g, '').replace(/-/g, ' ');
  
  return cleaned.trim();
}

export function extractTeamName(fullName: string): string {
  const parts = fullName.split(' ');
  if (parts.length >= 2) {
    if (parts.length >= 3 && parts[parts.length - 2].toLowerCase() === 'trail') {
      return parts.slice(-2).join(' ');
    }
    return parts[parts.length - 1];
  }
  return fullName;
}

