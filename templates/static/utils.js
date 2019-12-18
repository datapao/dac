function is_empty(obj) {
    return Object.keys(obj).length === 0 && obj.constructor === Object;
}