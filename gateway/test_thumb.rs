use teloxide::types::{InputMedia, InputMediaAudio, InputFile};

fn main() {
    let _ = InputMediaAudio::new(InputFile::file("foo.mp3")).thumb(InputFile::file("thumb.jpg"));
    let _ = InputMediaAudio::new(InputFile::file("foo.mp3")).thumbnail(InputFile::file("thumb.jpg"));
    let _ = InputMediaAudio::new(InputFile::file("foo.mp3")).cover(InputFile::file("thumb.jpg"));
}
